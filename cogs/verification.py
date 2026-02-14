import discord
from discord.ext import commands
from discord import app_commands

from datetime import datetime, timezone

from core import db
from core.config import load_config
from core.utils import utc_now_iso

CFG = load_config()
ADMIN_SERVER_ID = int(CFG.get("admin_server_id") or 0)
VERIFICATION_CHANNEL_ID = int(CFG.get("verification_channel_id") or 0)

ADMIN_GUILD_OBJ = discord.Object(id=ADMIN_SERVER_ID)


async def deny_if_blocked(interaction: discord.Interaction) -> bool:
    # Allow admins in control server to always use admin controls
    if interaction.guild and interaction.guild.id == ADMIN_SERVER_ID:
        if getattr(interaction.user, "guild_permissions", None) and interaction.user.guild_permissions.administrator:
            return False

    if db.is_user_blocked(interaction.user.id):
        try:
            if interaction.response.is_done():
                await interaction.followup.send("⛔ You are blocked from using FoxCom commands.", ephemeral=True)
            else:
                await interaction.response.send_message("⛔ You are blocked from using FoxCom commands.", ephemeral=True)
        except Exception:
            pass
        return True
    return False


# -----------------------------------------------------------------------------
# Notify a guild it was approved (best effort)
# -----------------------------------------------------------------------------
async def notify_guild_approved(bot: commands.Bot, guild_id: int, message: str) -> bool:
    """
    Try to notify a guild it was approved. Returns True if a message was sent somewhere.

    Order:
      1) Configured broadcast channel (channels table)
      2) System channel
      3) First text channel we can send in
      4) DM server owner (last resort)
    """
    guild = bot.get_guild(int(guild_id))
    if not guild:
        return False

    me = guild.me or guild.get_member(getattr(bot.user, "id", 0))

    # 1) Configured broadcast channel (channels table)
    try:
        conn = db.connect()
        cur = conn.cursor()
        cur.execute("SELECT channel_id FROM channels WHERE guild_id=?", (int(guild.id),))
        r = cur.fetchone()
        conn.close()

        if r and r["channel_id"]:
            chan_id = int(r["channel_id"])
            ch = guild.get_channel(chan_id)
            if isinstance(ch, (discord.TextChannel, discord.Thread)):
                perms = ch.permissions_for(me) if me else ch.permissions_for(guild.default_role)
                if perms.send_messages:
                    await ch.send(message)
                    return True
    except Exception:
        pass

    # 2) System channel
    if guild.system_channel:
        perms = guild.system_channel.permissions_for(me) if me else guild.system_channel.permissions_for(guild.default_role)
        if perms.send_messages:
            await guild.system_channel.send(message)
            return True

    # 3) Any text channel we can talk in
    for ch in guild.text_channels:
        perms = ch.permissions_for(me) if me else ch.permissions_for(guild.default_role)
        if perms.send_messages:
            await ch.send(message)
            return True

    # 4) DM owner
    try:
        owner = guild.owner
        if owner is None and guild.owner_id:
            owner = await bot.fetch_user(guild.owner_id)
        if owner:
            await owner.send(message)
            return True
    except Exception:
        pass

    return False


class VerifyDecisionView(discord.ui.View):
    def __init__(self, bot: commands.Bot, target_guild_id: int, requester_user_id: int | None = None):
        super().__init__(timeout=None)
        self.bot = bot
        self.target_guild_id = int(target_guild_id)
        self.requester_user_id = int(requester_user_id) if requester_user_id else None

    async def _guard_admin_in_control(self, interaction: discord.Interaction) -> bool:
        if await deny_if_blocked(interaction):
            return False
        if interaction.guild is None or interaction.guild.id != ADMIN_SERVER_ID:
            await interaction.response.send_message("❌ This can only be used in the FoxCom control server.", ephemeral=True)
            return False
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Admins only.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Approve", style=discord.ButtonStyle.success)
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard_admin_in_control(interaction):
            return

        req = db.get_pending(self.target_guild_id)
        if not req:
            await interaction.response.send_message("⚠️ That request is no longer pending.", ephemeral=True)
            return

        db.approve_guild(
            guild_id=self.target_guild_id,
            regiment=(req["regiment"] or "").strip(),
            server_name=req["server_name"] or "Unknown Server",
            requested_by=req["submitted_by"] or "Unknown",
            approved_by=str(interaction.user),
            approved_at=utc_now_iso(),
        )
        db.delete_pending(self.target_guild_id)

        # update the review embed
        try:
            msg = interaction.message
            if msg and msg.embeds:
                emb = msg.embeds[0]
                emb.color = discord.Color.green()
                emb.add_field(name="Status", value=f"✅ Approved by {interaction.user}", inline=False)
                await msg.edit(embed=emb, view=None)
        except Exception as e:
            print(f"Failed to edit approval message: {e}")

        # notify the approved server (best effort)
        try:
            reg = (req["regiment"] or "").strip()
            server_name = req["server_name"] or "your server"
            notify_msg = (
                "✅ **FoxCom verification approved!**\n"
                f"Server: **{server_name}**\n"
                f"Regiment/Org: **{reg or 'N/A'}**\n\n"
                "You can now use FoxCom broadcast features.\n"
                "If you haven’t already, set your broadcast channel with `/foxcomchannelset`."
            )
            await notify_guild_approved(self.bot, self.target_guild_id, notify_msg)
        except Exception as e:
            print(f"Failed to notify approved guild: {e}")

        # optionally DM requester
        if self.requester_user_id:
            try:
                u = await self.bot.fetch_user(self.requester_user_id)
                await u.send(
                    "✅ **Your FoxCom verification was approved!**\n"
                    "Next: run `/foxcomchannelset` in your server so FoxCom can send messages there."
                )
            except Exception:
                pass

        await interaction.response.send_message("✅ Approved and stored.", ephemeral=True)

    @discord.ui.button(label="Reject", style=discord.ButtonStyle.danger)
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard_admin_in_control(interaction):
            return

        req = db.get_pending(self.target_guild_id)
        if not req:
            await interaction.response.send_message("⚠️ That request is no longer pending.", ephemeral=True)
            return

        db.delete_pending(self.target_guild_id)

        try:
            msg = interaction.message
            if msg and msg.embeds:
                emb = msg.embeds[0]
                emb.color = discord.Color.red()
                emb.add_field(name="Status", value=f"❌ Rejected by {interaction.user}", inline=False)
                await msg.edit(embed=emb, view=None)
        except Exception as e:
            print(f"Failed to edit rejection message: {e}")

        # optionally DM requester
        if self.requester_user_id:
            try:
                u = await self.bot.fetch_user(self.requester_user_id)
                await u.send(
                    "❌ **Your FoxCom verification was rejected.**\n"
                    "If you think this was a mistake, re-run `/foxcomverify` and ensure you include a clear F1 screenshot."
                )
            except Exception:
                pass

        await interaction.response.send_message("✅ Rejected.", ephemeral=True)


def _is_image_attachment(att: discord.Attachment) -> bool:
    # Discord may provide content_type; also fall back to filename
    ct = (att.content_type or "").lower()
    if ct.startswith("image/"):
        return True
    name = (att.filename or "").lower()
    return name.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif"))


class VerificationCog(commands.Cog):
    """
    New verification flow:
      - User runs /foxcomverify in a server
      - Bot DMs user to collect regiment/org text
      - Bot then asks for an F1 screenshot (image attachment)
      - Bot forwards it (no DB storage) to FoxCom control channel with Approve/Reject buttons
      - Bot tells user to run /foxcomchannelset after submission
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="foxcomverify", description="Request access to FoxCom (you will be asked for an F1 screenshot via DM).")
    async def foxcomverify(self, interaction: discord.Interaction):
        if await deny_if_blocked(interaction):
            return
        if interaction.guild is None:
            await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
            return
        if VERIFICATION_CHANNEL_ID == 0:
            await interaction.response.send_message("❌ verification_channel_id not set in config.json.", ephemeral=True)
            return

        # Tell them to check DMs right away (fast response)
        try:
            await interaction.response.send_message("📩 Check your DMs to complete verification.", ephemeral=True)
        except Exception:
            return

        guild = interaction.guild
        requester = interaction.user

        # DM flow (best effort)
        try:
            dm = await requester.create_dm()
            await dm.send(
                "🛂 **FoxCom Verification**\n\n"
                "Reply with your **Regiment / Organization name** (text only).\n"
                "Then I’ll ask you to send an **F1 screenshot** from in-game as an image attachment."
            )
        except discord.Forbidden:
            try:
                await interaction.followup.send(
                    "❌ I couldn’t DM you. Please enable DMs from server members and run `/foxcomverify` again.",
                    ephemeral=True,
                )
            except Exception:
                pass
            return
        except Exception:
            return

        def check_text(m: discord.Message) -> bool:
            return m.author.id == requester.id and isinstance(m.channel, discord.DMChannel)

        def check_image(m: discord.Message) -> bool:
            if m.author.id != requester.id or not isinstance(m.channel, discord.DMChannel):
                return False
            return any(_is_image_attachment(a) for a in m.attachments)

        # Wait for regiment/org text
        try:
            msg_text: discord.Message = await self.bot.wait_for("message", check=check_text, timeout=15 * 60)
        except Exception:
            try:
                await dm.send("⏳ Verification timed out. Please run `/foxcomverify` again when ready.")
            except Exception:
                pass
            return

        regiment = (msg_text.content or "").strip()
        if not regiment:
            try:
                await dm.send("❌ I didn’t get any text. Please run `/foxcomverify` again and send your regiment/org name.")
            except Exception:
                pass
            return

        # Ask for screenshot
        try:
            await dm.send(
                "✅ Got it.\n\n"
                "Now send your **F1 screenshot** as an **image attachment** (PNG/JPG)."
            )
        except Exception:
            return

        # Wait for screenshot message
        try:
            msg_img: discord.Message = await self.bot.wait_for("message", check=check_image, timeout=15 * 60)
        except Exception:
            try:
                await dm.send("⏳ Screenshot step timed out. Please run `/foxcomverify` again when ready.")
            except Exception:
                pass
            return

        # Take first image attachment
        img_att = None
        for a in msg_img.attachments:
            if _is_image_attachment(a):
                img_att = a
                break

        if not img_att:
            try:
                await dm.send("❌ I didn’t detect an image attachment. Please run `/foxcomverify` again.")
            except Exception:
                pass
            return

        # Store pending request (no screenshot stored)
        db.set_pending(
            guild_id=guild.id,
            server_name=guild.name,
            submitted_by=str(requester),
            regiment=regiment,
            submitted_at=utc_now_iso(),
        )

        # Build embed for control server
        embed = discord.Embed(title="🛂 New Verification Request", color=discord.Color.gold())
        embed.add_field(name="Server", value=guild.name, inline=False)
        embed.add_field(name="Server ID", value=str(guild.id), inline=False)
        embed.add_field(name="Submitted By", value=f"{requester} (ID: {requester.id})", inline=False)
        embed.add_field(name="Regiment/Org", value=regiment, inline=False)
        embed.set_footer(text=datetime.now(timezone.utc).strftime("Requested on %Y-%m-%d %H:%M UTC"))

        # Send to FoxCom control channel (forward screenshot)
        channel = self.bot.get_channel(VERIFICATION_CHANNEL_ID)
        if not channel:
            db.delete_pending(guild.id)
            try:
                await dm.send("❌ I couldn’t reach the FoxCom control review channel. Please try again later.")
            except Exception:
                pass
            return

        try:
            file = await img_att.to_file()
            await channel.send(
                embed=embed,
                file=file,
                view=VerifyDecisionView(bot=self.bot, target_guild_id=guild.id, requester_user_id=requester.id),
            )
        except Exception as e:
            print(f"Failed to forward verification to control: {e}")
            db.delete_pending(guild.id)
            try:
                await dm.send("❌ Failed to submit your verification. Please try again later.")
            except Exception:
                pass
            return

        # Confirm to user + remind about channelset
        try:
            await dm.send(
                "✅ **Submitted!** Your request was sent to FoxCom Control for review.\n\n"
                "Next: run `/foxcomchannelset` in your server so FoxCom knows where to send messages."
            )
        except Exception:
            pass


async def setup(bot: commands.Bot):
    await bot.add_cog(VerificationCog(bot))
