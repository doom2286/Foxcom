# cogs/broadcasts.py  (BroadcastsCog)
import io
import discord
from discord.ext import commands
from discord import app_commands

from datetime import datetime, timezone, timedelta

from core import db
from core.config import load_config
from core.utils import contains_disallowed_mentions, parse_iso_utc

CFG = load_config()
ADMIN_SERVER_ID = int(CFG.get("admin_server_id") or 0)

# -----------------------------------------------------------------------------
# Google Translate (v2 REST via API key) settings
# -----------------------------------------------------------------------------
def _cfg_bool(v, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "on", "enabled")

GOOGLE_TRANSLATE_KEY = (CFG.get("google_translate_api_key") or "").strip()
TRANSLATE_ENABLED = _cfg_bool(CFG.get("translate_enabled"), default=True)
TRANSLATE_SHOW_ORIGINAL = _cfg_bool(CFG.get("translate_show_original"), default=True)
try:
    TRANSLATE_MIN_CHARS = int(CFG.get("translate_min_chars") or 12)
except Exception:
    TRANSLATE_MIN_CHARS = 12

# Try to import translation helper; fall back to no-op if missing
try:
    from core.translate import maybe_translate_to_english  # type: ignore
except Exception:
    async def maybe_translate_to_english(api_key: str, text: str, *, enabled: bool = True, min_chars: int = 12):
        return text, None, False, None


# -----------------------------------------------------------------------------
# Rep cosmetics
# -----------------------------------------------------------------------------
def _rep_badge(rep: int) -> str:
    if rep >= 100:
        return " 🌟"
    if rep >= 50:
        return " ★★★"
    if rep >= 25:
        return " ★★"
    if rep >= 10:
        return " ★"
    return ""


def _rep_color(rep: int) -> discord.Color:
    if rep >= 100:
        return discord.Color.gold()
    if rep >= 50:
        return discord.Color.purple()
    if rep >= 25:
        return discord.Color.blue()
    if rep >= 10:
        return discord.Color.green()
    return discord.Color.dark_grey()


def _format_regiment_tag(regiment: str | None, fallback: str) -> str:
    if regiment:
        reg = regiment.strip()
        if not (reg.startswith("[") and reg.endswith("]")):
            reg = f"[{reg}]"
        return reg
    return fallback


def _limits_for_rep(rep: int) -> tuple[int, int]:
    if rep <= -30:
        return 1, 4 * 60 * 60
    if rep <= -2:
        return 1, 60 * 60
    if rep <= 9:
        return 5, 60 * 60
    if rep <= 25:
        return 15, 60 * 60
    return 30, 60 * 60


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


def _in_control_server(interaction: discord.Interaction) -> bool:
    return interaction.guild is not None and interaction.guild.id == ADMIN_SERVER_ID


def _is_admin(interaction: discord.Interaction) -> bool:
    perms = getattr(interaction.user, "guild_permissions", None)
    return bool(perms and getattr(perms, "administrator", False))


async def _deny_if_blocked(interaction: discord.Interaction) -> bool:
    if interaction.guild and interaction.guild.id == ADMIN_SERVER_ID:
        perms = getattr(interaction.user, "guild_permissions", None)
        if perms and getattr(perms, "administrator", False):
            return False

    if db.is_user_blocked(interaction.user.id):
        try:
            msg = "⛔ You are blocked from using FoxCom commands."
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            pass
        return True

    return False


def _perm_report_for_channel(me: discord.Member | None, channel: discord.abc.GuildChannel | discord.Thread) -> str:
    """
    Returns a human-friendly permission report for the bot in the target channel.
    Works for TextChannel and Thread (effective perms for thread are based on parent + thread state).
    """
    if me is None:
        return "Bot member not available (cache)."

    base_channel = channel.parent if isinstance(channel, discord.Thread) else channel
    perms = base_channel.permissions_for(me)

    need = {
        "View Channel": perms.view_channel,
        "Send Messages": perms.send_messages,
        "Embed Links": perms.embed_links,
        "Attach Files": perms.attach_files,
        "Read Message History": perms.read_message_history,
        "Send Messages in Threads": getattr(perms, "send_messages_in_threads", True),
    }

    lines = []
    for k, v in need.items():
        lines.append(f"{'✅' if v else '❌'} {k}")

    extra = []
    if isinstance(channel, discord.Thread):
        extra.append(f"Thread archived: {channel.archived}")
        extra.append(f"Thread locked: {channel.locked}")

    report = " | ".join(lines)
    if extra:
        report += "\n" + " • " + "\n • ".join(extra)
    return report


class BroadcastsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # Uses the existing channels table via db.all_channels()
    def _get_broadcast_channel_id_for_guild(self, guild_id: int) -> int | None:
        try:
            for gid, cid in db.all_channels():
                if int(gid) == int(guild_id):
                    return int(cid)
        except Exception:
            pass
        return None

    def _get_report_channel(self) -> discord.TextChannel | None:
        report_channel_id = int(CFG.get("report_channel_id") or 0)
        if not report_channel_id:
            return None
        ch = self.bot.get_channel(report_channel_id)
        return ch if isinstance(ch, discord.TextChannel) else None

    def _find_fallback_channel(self, guild: discord.Guild) -> discord.TextChannel | None:
        """
        If a guild doesn't have /foxcomchannelset configured, find a channel the bot can post in.
        Preference order:
          1) system_channel (if sendable)
          2) first text channel with view+send perms
        """
        me = guild.me  # type: ignore
        if guild.system_channel and me:
            try:
                perms = guild.system_channel.permissions_for(me)
                if perms.view_channel and perms.send_messages:
                    return guild.system_channel
            except Exception:
                pass

        if not me:
            return None

        for ch in guild.text_channels:
            try:
                perms = ch.permissions_for(me)
                if perms.view_channel and perms.send_messages:
                    return ch
            except Exception:
                continue
        return None

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

        # Optional: word filter (pre-translation pass)
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
                pass

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        # --- Optional auto-translation to English (Google Translate v2 via API key) ---
        translated_msg = message
        detected_lang = None
        did_translate = False
        original_msg = None

        # Global toggle (bot-wide) + local config toggle must both be enabled
        global_translate_enabled = True
        try:
            global_translate_enabled = db.get_global_translation_enabled(default=True)
        except Exception:
            global_translate_enabled = True

        if TRANSLATE_ENABLED and global_translate_enabled and GOOGLE_TRANSLATE_KEY:
            try:
                translated_msg, detected_lang, did_translate, original_msg = await maybe_translate_to_english(
                    GOOGLE_TRANSLATE_KEY,
                    message,
                    enabled=True,
                    min_chars=TRANSLATE_MIN_CHARS
                )
            except Exception as e:
                print(f"[translate] failed: {e}")
                translated_msg = message
                detected_lang = None
                did_translate = False
                original_msg = None

        # Optional: word filter (post-translation pass)
        if did_translate and wf and hasattr(wf, "check_text"):
            try:
                hit2 = wf.check_text(translated_msg)
                if hit2:
                    await interaction.followup.send(
                        "This broadcast was blocked after translation (banned word/phrase detected).",
                        ephemeral=True
                    )
                    return
            except Exception:
                pass

        now = datetime.now(timezone.utc)

        if _should_prune_rep(now):
            try:
                db.prune_rep()
            except Exception as e:
                print(f"[rep] prune_rep failed: {e}")

        try:
            db.ensure_rep_user(interaction.user.id, str(interaction.user))
        except Exception as e:
            print(f"[rep] ensure_rep_user failed: {e}")

        sender_rep = 0
        try:
            sender_rep = int(db.get_rep(interaction.user.id) or 0)
        except Exception:
            sender_rep = 0

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

        color = _rep_color(sender_rep)
        badge = _rep_badge(sender_rep)

        embed = discord.Embed(
            title=f"{sender_prefix} {tag}",
            description=translated_msg,
            color=color
        )
        try:
            embed.set_author(name=str(interaction.user), icon_url=interaction.user.display_avatar.url)
        except Exception:
            embed.set_author(name=str(interaction.user))

        if did_translate and TRANSLATE_SHOW_ORIGINAL and original_msg:
            embed.add_field(
                name=f"Original ({detected_lang})" if detected_lang else "Original",
                value=original_msg[:1024],
                inline=False
            )

        marker = f"fc|a:{interaction.user.id}|g:{interaction.guild.id}|t:{int(now.timestamp())}"
        footer_human = f"Sent by {interaction.user} | From {interaction.guild.name} | Rep {sender_rep}{badge}"
        embed.set_footer(text=f"{footer_human}  {marker}")

        sent_count = 0

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

                try:
                    db.track_rep_message(sent.id, interaction.user.id, str(interaction.user))
                except Exception as e:
                    print(f"[rep] track_rep_message failed: {e}")

            except Exception as e:
                try:
                    g = self.bot.get_guild(int(guild_id))
                    me = g.me if g else None  # type: ignore
                    if g and channel_id:
                        ch = g.get_channel(int(channel_id))
                        if isinstance(ch, (discord.TextChannel, discord.Thread)):
                            print(f"[perm] guild {guild_id} channel {channel_id} -> {_perm_report_for_channel(me, ch)}")
                except Exception:
                    pass
                print(f"Failed to send to guild {guild_id}: {e}")

        await interaction.followup.send(
            f"Sent {tag} alert to {sent_count} approved server(s).",
            ephemeral=True
        )

    # -------------------- NEW: Admin Broadcast (control server only) --------------------
    async def _admin_broadcast(self, interaction: discord.Interaction, message: str):
        if await _deny_if_blocked(interaction):
            return

        if not interaction.guild:
            await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
            return

        if not _in_control_server(interaction):
            await interaction.response.send_message("❌ Use this in the FoxCom control server only.", ephemeral=True)
            return

        if not _is_admin(interaction):
            await interaction.response.send_message("❌ Admins only.", ephemeral=True)
            return

        if contains_disallowed_mentions(message):
            await interaction.response.send_message(
                "Mentions are not allowed (no @everyone, @here, roles, user pings, or '@').",
                ephemeral=True
            )
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        now = datetime.now(timezone.utc)

        embed = discord.Embed(
            title="🛠️ FOXCOM ADMIN",
            description=message,
            color=discord.Color.red()
        )
        try:
            embed.set_author(name=str(interaction.user), icon_url=interaction.user.display_avatar.url)
        except Exception:
            embed.set_author(name=str(interaction.user))

        marker = f"fc|admin:1|a:{interaction.user.id}|g:{interaction.guild.id}|t:{int(now.timestamp())}"
        embed.set_footer(text=f"Admin broadcast by {interaction.user}  {marker}")

        # Build a quick lookup for configured broadcast channels
        chan_map: dict[int, int] = {}
        try:
            for gid, cid in db.all_channels():
                chan_map[int(gid)] = int(cid)
        except Exception:
            chan_map = {}

        sent_count = 0
        fallback_count = 0
        no_channel_count = 0

        # Send to ALL guilds the bot is in (approved or not)
        for g in list(self.bot.guilds):
            try:
                target = None

                configured_id = chan_map.get(int(g.id))
                if configured_id:
                    ch = self.bot.get_channel(int(configured_id))
                    if isinstance(ch, (discord.TextChannel, discord.Thread)):
                        target = ch

                if target is None:
                    fb = self._find_fallback_channel(g)
                    if fb:
                        target = fb
                        fallback_count += 1
                    else:
                        no_channel_count += 1
                        continue

                await target.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
                sent_count += 1

            except Exception as e:
                print(f"[admin] Failed to send to guild {g.id} ({g.name}): {e}")

        await interaction.followup.send(
            f"✅ Admin broadcast sent to **{sent_count}** server(s).\n"
            f"• Used fallback channel in: **{fallback_count}**\n"
            f"• No accessible channel in: **{no_channel_count}**",
            ephemeral=True
        )

    # -------------------- Broadcast Commands --------------------
    @app_commands.command(name="qrf", description="Quick Reaction Force broadcast.")
    async def qrf(self, interaction: discord.Interaction, message: str):
        await self._broadcast_alert(interaction, "QRF", message)

    @app_commands.command(name="logi", description="Logistics request broadcast.")
    async def logi(self, interaction: discord.Interaction, message: str):
        await self._broadcast_alert(interaction, "LOGI", message)

    @app_commands.command(name="battle", description="Battle update broadcast.")
    async def battle(self, interaction: discord.Interaction, message: str):
        await self._broadcast_alert(interaction, "BATTLE", message)

    @app_commands.command(
        name="admin",
        description="(Control server admins only) Broadcast an admin announcement to all servers."
    )
    async def admin(self, interaction: discord.Interaction, message: str):
        await self._admin_broadcast(interaction, message)

    # -------------------- Local Test Broadcast (THIS server only) --------------------
    @app_commands.command(
        name="foxcomtest",
        description="Send a test FoxCom broadcast to THIS server only (no cross-server broadcast)."
    )
    async def foxcomtest(self, interaction: discord.Interaction):
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

        bc_channel_id = self._get_broadcast_channel_id_for_guild(interaction.guild.id)
        if not bc_channel_id:
            await interaction.response.send_message(
                "⚠️ This server has no FoxCom broadcast channel set. Ask an admin to run /foxcomchannelset.",
                ephemeral=True
            )
            return

        channel = interaction.guild.get_channel(int(bc_channel_id))
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(int(bc_channel_id))
            except discord.NotFound:
                await interaction.response.send_message(
                    f"⚠️ Saved broadcast channel id `{bc_channel_id}` no longer exists. Re-run /foxcomchannelset.",
                    ephemeral=True
                )
                return
            except discord.Forbidden:
                await interaction.response.send_message(
                    "⚠️ I can’t access the saved broadcast channel. Check channel permissions (View Channel).",
                    ephemeral=True
                )
                return
            except Exception as e:
                await interaction.response.send_message(f"⚠️ Failed to load channel: {e}", ephemeral=True)
                return

        if not isinstance(channel, (discord.TextChannel, discord.Thread)):
            await interaction.response.send_message(
                "⚠️ The configured broadcast channel isn't a normal text channel/thread. Re-run /foxcomchannelset.",
                ephemeral=True
            )
            return

        me = interaction.guild.me  # type: ignore
        perm_report = _perm_report_for_channel(me, channel)

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        now = datetime.now(timezone.utc)
        sender_rep = 0
        try:
            sender_rep = int(db.get_rep(interaction.user.id) or 0)
        except Exception:
            sender_rep = 0

        regiment = db.get_regiment(interaction.guild.id)
        sender_prefix = _format_regiment_tag(regiment, interaction.guild.name)

        embed = discord.Embed(
            title=f"{sender_prefix} TEST",
            description="✅ This is a local test broadcast. Only this server should receive it.",
            color=_rep_color(sender_rep)
        )
        try:
            embed.set_author(name=str(interaction.user), icon_url=interaction.user.display_avatar.url)
        except Exception:
            embed.set_author(name=str(interaction.user))

        badge = _rep_badge(sender_rep)
        marker = f"fc|a:{interaction.user.id}|g:{interaction.guild.id}|t:{int(now.timestamp())}"
        footer_human = f"Sent by {interaction.user} | From {interaction.guild.name} | Rep {sender_rep}{badge}"
        embed.set_footer(text=f"{footer_human}  {marker}")

        try:
            sent = await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
            try:
                db.track_rep_message(sent.id, interaction.user.id, str(interaction.user))
            except Exception as e:
                print(f"[rep] track_rep_message failed (test): {e}")

        except discord.Forbidden:
            await interaction.followup.send(
                "❌ I can't post in the configured broadcast channel.\n"
                f"**Channel:** {channel.mention} (`{bc_channel_id}`)\n"
                f"**Bot perms:**\n{perm_report}",
                ephemeral=True
            )
            return
        except discord.HTTPException as e:
            await interaction.followup.send(
                "❌ Discord rejected the message.\n"
                f"**Channel:** {channel.mention} (`{bc_channel_id}`)\n"
                f"**Error:** `{e}`\n"
                f"**Bot perms:**\n{perm_report}",
                ephemeral=True
            )
            return
        except Exception as e:
            await interaction.followup.send(
                "❌ Failed to send test broadcast.\n"
                f"**Channel:** {channel.mention} (`{bc_channel_id}`)\n"
                f"**Error:** `{e}`\n"
                f"**Bot perms:**\n{perm_report}",
                ephemeral=True
            )
            return

        await interaction.followup.send(
            f"✅ Sent test broadcast to {channel.mention}.\n**Bot perms:**\n{perm_report}",
            ephemeral=True
        )

    # -------------------- Reporting (scrape last hour, current guild only) --------------------
    @app_commands.command(
        name="foxcomreport",
        description="Report a user; attaches last hour of FoxCom broadcasts from this server to FoxCom staff."
    )
    @app_commands.describe(user="User being reported", reason="Optional reason")
    async def foxcomreport(self, interaction: discord.Interaction, user: discord.User, reason: str = ""):
        if await _deny_if_blocked(interaction):
            return

        if not interaction.guild:
            await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
            return

        if not db.is_guild_approved(interaction.guild.id):
            await interaction.response.send_message(
                "❌ This server is not approved to use FoxCom. Use /foxcomverify to request access.",
                ephemeral=True
            )
            return

        bc_channel_id = self._get_broadcast_channel_id_for_guild(interaction.guild.id)
        if not bc_channel_id:
            await interaction.response.send_message(
                "⚠️ This server has no FoxCom broadcast channel set. Ask an admin to run /foxcomchannelset.",
                ephemeral=True
            )
            return

        bc_channel = self.bot.get_channel(int(bc_channel_id))
        if not isinstance(bc_channel, (discord.TextChannel, discord.Thread)):
            await interaction.response.send_message(
                "⚠️ I couldn't access the configured broadcast channel. Check the stored channel ID + permissions.",
                ephemeral=True
            )
            return

        report_channel = self._get_report_channel()
        if not report_channel:
            await interaction.response.send_message(
                "⚠️ Reports are not configured (missing report_channel_id). Contact FoxCom staff.",
                ephemeral=True
            )
            return

        if not interaction.response.is_done():
            await interaction.response.send_message("✅ Your report has been submitted for review.", ephemeral=True)

        cutoff = datetime.now(timezone.utc) - timedelta(hours=1)
        needle = f"fc|a:{user.id}|"

        scanned = 0
        matched = 0
        collected: list[str] = []

        try:
            async for msg in bc_channel.history(limit=250, after=cutoff, oldest_first=False):
                scanned += 1

                if not self.bot.user or msg.author.id != self.bot.user.id:
                    continue
                if not msg.embeds:
                    continue

                e = msg.embeds[0]
                footer_text = (e.footer.text if e.footer else "") or ""
                if needle not in footer_text:
                    continue

                matched += 1

                created = msg.created_at.replace(tzinfo=timezone.utc).isoformat()
                title = e.title or "(no title)"
                desc = e.description or "(no description)"

                collected.append(
                    f"[{created}]\n"
                    f"Title: {title}\n"
                    f"Message: {desc}\n"
                    f"Footer: {footer_text}\n"
                    f"Link: {msg.jump_url}\n"
                    f"{'-' * 60}\n"
                )

        except discord.Forbidden:
            await interaction.followup.send(
                "⚠️ I don't have permission to read message history in the broadcast channel.",
                ephemeral=True
            )
            return
        except Exception as e:
            await interaction.followup.send(f"⚠️ Failed to collect messages: {e}", ephemeral=True)
            return

        header = (
            "FoxCom Report Export\n"
            f"Guild: {interaction.guild.name} ({interaction.guild.id})\n"
            f"Broadcast Channel: #{getattr(bc_channel, 'name', 'unknown')} ({bc_channel_id})\n"
            f"Reported User: {user} ({user.id})\n"
            f"Reported By: {interaction.user} ({interaction.user.id})\n"
            f"Reason: {reason.strip() or 'N/A'}\n"
            "Window: last 1 hour\n"
            f"Scanned: {scanned} | Matched: {matched}\n"
            f"{'=' * 60}\n\n"
        )

        body = header + ("".join(collected) if collected else "No matching FoxCom broadcasts found in the last hour.\n")
        file = discord.File(
            io.BytesIO(body.encode("utf-8", errors="replace")),
            filename=f"foxcom_report_{interaction.guild.id}_{user.id}.txt"
        )

        embed = discord.Embed(
            title="🚩 FoxCom User Report",
            description=(
                f"**Reported User:** {user.mention} (`{user.id}`)\n"
                f"**Reported By:** {interaction.user.mention} (`{interaction.user.id}`)\n"
                f"**Server:** {interaction.guild.name} (`{interaction.guild.id}`)\n"
                f"**Reason:** {reason.strip() or 'N/A'}\n"
                f"**Found broadcasts (last hour):** {matched}"
            ),
            color=discord.Color.orange()
        )

        await report_channel.send(embed=embed, file=file, allowed_mentions=discord.AllowedMentions.none())


async def setup(bot: commands.Bot):
    await bot.add_cog(BroadcastsCog(bot))
