import discord
from discord.ext import commands
from discord import app_commands
from core import db
from core.config import load_config
from core.utils import utc_now_iso
from datetime import datetime, timezone

CFG = load_config()
ADMIN_SERVER_ID = int(CFG.get("admin_server_id") or 0)
VERIFICATION_CHANNEL_ID = int(CFG.get("verification_channel_id") or 0)

ADMIN_GUILD_OBJ = discord.Object(id=ADMIN_SERVER_ID)


async def deny_if_blocked(interaction: discord.Interaction) -> bool:
    if interaction.guild and interaction.guild.id == ADMIN_SERVER_ID:
        if getattr(interaction.user, "guild_permissions", None) and interaction.user.guild_permissions.administrator:
            return False

    if db.is_user_blocked(interaction.user.id):
        try:
            if interaction.response.is_done():
                await interaction.followup.send("⛔ You are blocked from using FoxCom commands.", ephemeral=True)
            else:
                await interaction.response.send_message("⛔ You are blocked from using FoxCom commands.", ephemeral=True)
        except:
            pass
        return True
    return False


class VerifyDecisionView(discord.ui.View):
    def __init__(self, target_guild_id: int):
        super().__init__(timeout=None)
        self.target_guild_id = int(target_guild_id)

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

        await interaction.response.send_message("✅ Rejected.", ephemeral=True)


class VerifyModal(discord.ui.Modal, title="FoxCom Verification"):
    regiment = discord.ui.TextInput(
        label="Your Regiment or Organization",
        placeholder="e.g., 82DK, Warden Tactical Unit",
        max_length=100
    )

    def __init__(self, bot: commands.Bot):
        super().__init__()
        self.bot = bot

    async def on_submit(self, interaction: discord.Interaction):
        if await deny_if_blocked(interaction):
            return

        if not interaction.guild:
            await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
            return

        if VERIFICATION_CHANNEL_ID == 0:
            await interaction.response.send_message("❌ verification_channel_id not set in config.json.", ephemeral=True)
            return

        reg = self.regiment.value.strip()
        db.set_pending(
            guild_id=interaction.guild.id,
            server_name=interaction.guild.name,
            submitted_by=str(interaction.user),
            regiment=reg,
            submitted_at=utc_now_iso(),
        )

        embed = discord.Embed(title="🛂 New Verification Request", color=discord.Color.gold())
        embed.add_field(name="Server", value=interaction.guild.name, inline=False)
        embed.add_field(name="Submitted By", value=str(interaction.user), inline=False)
        embed.add_field(name="Regiment/Org", value=reg, inline=False)
        embed.add_field(name="Server ID", value=str(interaction.guild.id), inline=False)
        embed.set_footer(text=datetime.now(timezone.utc).strftime("Requested on %Y-%m-%d %H:%M UTC"))

        channel = self.bot.get_channel(VERIFICATION_CHANNEL_ID)
        if not channel:
            await interaction.response.send_message("❌ Could not find the FoxCom review channel.", ephemeral=True)
            return

        await channel.send(embed=embed, view=VerifyDecisionView(target_guild_id=interaction.guild.id))
        await interaction.response.send_message("✅ Your verification request has been submitted.", ephemeral=True)


class VerificationCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="foxcomverify", description="Request access to FoxCom by submitting your regiment/org.")
    async def foxcomverify(self, interaction: discord.Interaction):
        if await deny_if_blocked(interaction):
            return
        await interaction.response.send_modal(VerifyModal(self.bot))


async def setup(bot: commands.Bot):
    await bot.add_cog(VerificationCog(bot))

