import discord
from discord.ext import commands, tasks
from discord import app_commands

from core import db
from core.config import load_config

CFG = load_config()
ADMIN_SERVER_ID = int(CFG.get("admin_server_id") or 0)

# User-provided reinstall link
REINSTALL_LINK = "https://discord.com/oauth2/authorize?client_id=1400433838894743572"

# Permissions FoxCom needs in the broadcast channel (and generally any channel it posts in)
REQUIRED_CHANNEL_PERMS = (
    "view_channel",
    "send_messages",
    "embed_links",
    "attach_files",
)


def _missing_perms(perms: discord.Permissions) -> list[str]:
    missing: list[str] = []
    for name in REQUIRED_CHANNEL_PERMS:
        if not getattr(perms, name, False):
            missing.append(name)
    return missing


def _format_missing(missing: list[str]) -> str:
    pretty = {
        "view_channel": "View Channel",
        "send_messages": "Send Messages",
        "embed_links": "Embed Links",
        "attach_files": "Attach Files",
        "add_reactions": "Add Reactions",
        "use_external_emojis": "Use External Emojis",
        "send_messages_in_threads": "Send Messages in Threads",
    }
    return ", ".join(pretty.get(m, m) for m in missing)


def _guild_me(guild: discord.Guild, bot: commands.Bot) -> discord.Member | None:
    return guild.me or guild.get_member(getattr(bot.user, "id", 0))


async def _fetch_configured_channel_id(guild_id: int) -> int | None:
    """Read channels.channel_id for a guild (your /foxcomchannelset table)."""
    try:
        conn = db.connect()
        cur = conn.cursor()
        cur.execute("SELECT channel_id FROM channels WHERE guild_id=?", (int(guild_id),))
        r = cur.fetchone()
        conn.close()
        if r and r["channel_id"]:
            return int(r["channel_id"])
    except Exception:
        pass
    return None


async def _try_send_anywhere(guild: discord.Guild, bot: commands.Bot, content: str) -> bool:
    """Try to send a message somewhere in the guild we have permissions."""
    me = _guild_me(guild, bot)
    if me is None:
        return False

    # 1) system channel
    if guild.system_channel:
        perms = guild.system_channel.permissions_for(me)
        if perms.send_messages and perms.view_channel:
            try:
                await guild.system_channel.send(content)
                return True
            except Exception:
                pass

    # 2) any text channel
    for ch in guild.text_channels:
        perms = ch.permissions_for(me)
        if perms.send_messages and perms.view_channel:
            try:
                await ch.send(content)
                return True
            except Exception:
                continue

    return False


async def _dm_owner(bot: commands.Bot, guild: discord.Guild, content: str) -> bool:
    try:
        owner = guild.owner
        if owner is None and guild.owner_id:
            owner = await bot.fetch_user(guild.owner_id)
        if owner:
            await owner.send(content)
            return True
    except Exception:
        pass
    return False


class AuditCog(commands.Cog):
    """Daily permission audit + manual /runaudit trigger."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.daily_permission_audit.start()

    def cog_unload(self):
        self.daily_permission_audit.cancel()

    # -------------------------
    # Manual trigger: /runaudit
    # -------------------------
    @app_commands.command(name="runaudit", description="Manually run a FoxCom permission audit for this server.")
    @app_commands.default_permissions(administrator=True)
    async def runaudit(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("❌ This must be run in a server.", ephemeral=True)
            return

        # Optional: only let admins / manage_guild run it.
        # default_permissions(administrator=True) already hides it from non-admins,
        # but also enforce at runtime in case permissions cache differs.
        if not getattr(interaction.user, "guild_permissions", None) or not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Admins only.", ephemeral=True)
            return

        await interaction.response.send_message("🔎 Running audit…", ephemeral=True)

        ok, details, notified = await self._audit_guild(interaction.guild, manual=True)

        if ok:
            await interaction.followup.send("✅ Audit complete: FoxCom permissions look good.", ephemeral=True)
        else:
            extra = "I attempted to notify your server/owner." if notified else "I could not notify any channel or DM the owner."
            await interaction.followup.send(f"⚠️ Audit found an issue: **{details}**\n{extra}", ephemeral=True)

    # -------------------------
    # Scheduled daily audit
    # -------------------------
    @tasks.loop(hours=24)
    async def daily_permission_audit(self):
        for guild in list(self.bot.guilds):
            try:
                await self._audit_guild(guild, manual=False)
            except Exception as e:
                print(f"[audit] error auditing {guild.id}: {e}")

    @daily_permission_audit.before_loop
    async def before_daily_permission_audit(self):
        await self.bot.wait_until_ready()

    async def _audit_guild(self, guild: discord.Guild, manual: bool = False) -> tuple[bool, str, bool]:
        """
        Returns: (ok, details, notified)
          ok=True  -> no problems found
          ok=False -> details describes problem; notified indicates whether we messaged a channel or owner
        """
        me = _guild_me(guild, self.bot)
        if me is None:
            return False, "Bot member not found in guild", False

        # Prefer checking the configured broadcast channel (if any)
        configured_id = await _fetch_configured_channel_id(guild.id)
        configured_name = None

        issue = None

        if configured_id:
            configured_ch = guild.get_channel(configured_id)
            if isinstance(configured_ch, discord.TextChannel):
                configured_name = f"#{configured_ch.name}"
                perms = configured_ch.permissions_for(me)
                missing = _missing_perms(perms)
                if missing:
                    issue = f"Missing permissions in {configured_name}: {_format_missing(missing)}"
            else:
                issue = "Configured broadcast channel is missing or not a text channel"
                configured_name = f"(channel id {configured_id})"
        else:
            configured_name = "(no broadcast channel set)"
            # If no configured channel, at least ensure the bot can speak somewhere
            can_speak = False
            if guild.system_channel:
                p = guild.system_channel.permissions_for(me)
                can_speak = can_speak or (p.view_channel and p.send_messages)
            for ch in guild.text_channels:
                p = ch.permissions_for(me)
                if p.view_channel and p.send_messages:
                    can_speak = True
                    break
            if not can_speak:
                issue = "No accessible text channel to send messages"

        if not issue:
            return True, "", False

        msg = (
            "⚠️ **FoxCom Permission Audit**\n"
            f"Server: **{guild.name}** (`{guild.id}`)\n"
            f"Broadcast channel: **{configured_name or 'Unknown'}**\n"
            f"Issue: **{issue}**\n\n"
            "Fix options:\n"
            "• Grant FoxCom the needed permissions in the broadcast channel (recommended), then run `/foxcomchannelset` again if needed.\n"
            f"• If you prefer, you can kick FoxCom and reinstall it using this link: {REINSTALL_LINK}"
        )

        notified = await _try_send_anywhere(guild, self.bot, msg)
        if not notified:
            notified = await _dm_owner(self.bot, guild, msg)

        return False, issue, notified


async def setup(bot: commands.Bot):
    await bot.add_cog(AuditCog(bot))
