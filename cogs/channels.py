import discord
from discord.ext import commands
from discord import app_commands
from core import db
from core.config import load_config

CFG = load_config()
ADMIN_SERVER_ID = int(CFG.get("admin_server_id") or 0)


async def deny_if_blocked(interaction: discord.Interaction) -> bool:
    # allow control-server admins to bypass blocks
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


class ChannelsCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_guild_join(self, guild: discord.Guild):
        welcome = (
            "🦊 Hello, I am **FoxCom**!\n\n"
            "To get started:\n"
            "1️⃣ Use `/foxcomchannelset` in a text channel to set where alerts go\n"
            "2️⃣ Use `/foxcomverify` to submit your regiment/org for approval\n\n"
            "Verification is required before using broadcast commands."
        )
        channel = guild.system_channel
        if not channel:
            for c in guild.text_channels:
                if c.permissions_for(guild.me).send_messages:
                    channel = c
                    break
        if channel:
            try:
                await channel.send(welcome)
            except:
                pass

    @app_commands.command(name="foxcomchannelset", description="Set this channel to receive FoxCom alerts.")
    async def foxcomchannelset(self, interaction: discord.Interaction):
        if await deny_if_blocked(interaction):
            return

        if not interaction.guild:
            await interaction.response.send_message("❌ Must be used in a server.", ephemeral=True)
            return

        db.set_channel(interaction.guild.id, interaction.channel.id)
        await interaction.response.send_message("✅ This channel is now set for FoxCom alerts.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(ChannelsCog(bot))

