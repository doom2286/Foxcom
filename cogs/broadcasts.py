import discord
from discord.ext import commands
from discord import app_commands

from datetime import datetime, timezone

from core import db
from core.config import load_config
from core.utils import contains_disallowed_mentions, parse_iso_utc

CFG = load_config()
ADMIN_SERVER_ID = int(CFG.get("admin_server_id") or 0)


def _format_regiment_tag(regiment: str | None, fallback: str) -> str:
    if regiment:
        reg = regiment.strip()
        if not (reg.startswith("[") and reg.endswith("]")):
            reg = f"[{reg}]"
        return reg
    return fallback


def _limits_for_rep(rep: int) -> tuple[int, int]:
    """
    Return (max_actions, window_seconds) based on reputation.
    """
    if rep <= -30:
        return 1, 4 * 60 * 60     # 1 per 4 hours
    if rep <= -2:
        return 1, 60 * 60         # 1 per hour
    if rep <= 9:
        return 5, 60 * 60         # 5 per hour
    if rep <= 25:
        return 15, 60 * 60        # 15 per hour
    return 30, 60 * 60            # 30 per hour


def _format_wait(retry_after: int) -> str:
    mins = retry_after // 60
    secs = retry_after % 60
    if mins >= 60:
        hrs = mins // 60
        mins = mins % 60
        return f"{hrs}h {mins}m"
    if mins > 0:
        return f"{mins}m {secs}s"
    return f"{secs}s"


def _should_prune_rep(now_utc: datetime, min_interval_seconds: int = 10 * 60) -> bool:
    """
    Avoid pruning on every broadcast; it causes extra writes and lock contention.
    """
    try:
        last = db.get_last_prune()
        if not last:
            return True
        t = parse_iso_utc(last)
        if not t:
            return True
        return (now_utc - t).total_seconds() >= min_interval_seconds
    except Exception:
        return False


async def _deny_if_blocked(interaction: discord.Interaction) -> bool:
    # Allow admins in the admin server to bypass user blocks
    if interaction.guild and interaction.guild.id == ADMIN_SERVER_ID:
        perms = getattr(interaction.user, "guild_permissions", None)
        if perms and getattr(perms, "administrator", False):
            return False

    if db.is_user_blocked(interaction.user.id):
        try:
            msg = "You are blocked from using FoxCom commands."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            pass
        return True

    return False


class BroadcastsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def _broadcast_alert(self, interaction: discord.Interaction, tag: str, message: str):
        if await _deny_if_blocked(interaction):
            return

        if not interaction.guild:
            await interaction.response.send_message("Must be used in a server.", ephemeral=True)
            return

        if not db.is_guild_approved(interaction.guild.id):
            await interaction.response.send_message(
                "This server is not approved to use FoxCom. Use /foxcomverify to request access.",
                ephemeral=True
            )
            return

        # Mention / ping suppression
        if contains_disallowed_mentions(message):
            await interaction.response.send_message(
                "Mentions are not allowed (no @everyone, @here, roles, user pings, or '@').",
                ephemeral=True
            )
            return

        # Optional: word filter (if your WordFilterCog exposes check_text)
        wf = self.bot.get_cog("WordFilterCog")
        if wf:
            try:
                if hasattr(wf, "reload_cfg"):
                    wf.reload_cfg()
                if hasattr(wf, "check_text"):
                    hit = wf.check_text(message)
                    if hit:
                        await interaction.response.send_message(
                            "This broadcast contains a banned word/phrase and was blocked.",
                            ephemeral=True
                        )
                        return
            except Exception:
                # Do not fail broadcasts if filter errors
                pass

        # IMPORTANT: defer quickly so Discord doesn't show "did not respond"
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        now = datetime.now(timezone.utc)

        # Prune rep tracking occasionally (not every time)
        if _should_prune_rep(now):
            try:
                db.prune_rep()
            except Exception as e:
                print(f"[rep] prune_rep failed: {e}")

        # Ensure rep record exists
        try:
            db.ensure_rep_user(interaction.user.id, str(interaction.user))
        except Exception as e:
            print(f"[rep] ensure_rep_user failed: {e}")

        sender_rep = 0
        try:
            sender_rep = int(db.get_rep(interaction.user.id) or 0)
        except Exception:
            sender_rep = 0

        # Rep-based broadcast quota
        max_actions, window_seconds = _limits_for_rep(sender_rep)
        allowed, retry_after = db.check_and_consume_broadcast_quota(
            interaction.user.id, max_actions, window_seconds
        )
        if not allowed:
            wait_str = _format_wait(int(retry_after))
            await interaction.followup.send(
                f"Rate limit hit for your rep tier. Try again in {wait_str}.",
                ephemeral=True
            )
            return

        regiment = db.get_regiment(interaction.guild.id)
        sender_prefix = _format_regiment_tag(regiment, interaction.guild.name)

        embed = discord.Embed(
            title=f"{sender_prefix} {tag}",
            description=message
        )
        try:
            embed.set_author(name=str(interaction.user), icon_url=interaction.user.display_avatar.url)
        except Exception:
            embed.set_author(name=str(interaction.user))

        embed.set_footer(text=f"Rep: {sender_rep} | From {interaction.guild.name} ({interaction.guild.id})")

        sent_count = 0

        # Broadcast to all approved servers' configured channels
        for guild_id, channel_id in db.all_channels():
            if not db.is_guild_approved(guild_id):
                continue

            try:
                channel = self.bot.get_channel(channel_id)
                if not channel:
                    continue

                sent = await channel.send(
                    embed=embed,
                    allowed_mentions=discord.AllowedMentions.none()
                )
                sent_count += 1

                # Track for rep reactions
                try:
                    db.track_rep_message(sent.id, interaction.user.id, str(interaction.user))
                except Exception as e:
                    print(f"[rep] track_rep_message failed: {e}")

            except Exception as e:
                print(f"Failed to send to guild {guild_id}: {e}")

        await interaction.followup.send(
            f"Sent {tag} alert to {sent_count} approved server(s).",
            ephemeral=True
        )

    @app_commands.command(name="qrf", description="Quick Reaction Force broadcast.")
    async def qrf(self, interaction: discord.Interaction, message: str):
        await self._broadcast_alert(interaction, "QRF", message)

    @app_commands.command(name="logi", description="Logistics request broadcast.")
    async def logi(self, interaction: discord.Interaction, message: str):
        await self._broadcast_alert(interaction, "LOGI", message)

    @app_commands.command(name="battle", description="Battle update broadcast.")
    async def battle(self, interaction: discord.Interaction, message: str):
        await self._broadcast_alert(interaction, "BATTLE", message)


async def setup(bot: commands.Bot):
    await bot.add_cog(BroadcastsCog(bot))
