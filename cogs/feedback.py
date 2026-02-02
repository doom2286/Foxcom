import discord
from discord.ext import commands
from discord import app_commands
from core import db
from core.config import load_config
from core.utils import contains_disallowed_mentions
from datetime import datetime, timezone

CFG = load_config()
ADMIN_SERVER_ID = int(CFG.get("admin_server_id") or 0)
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


class FeedbackCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # ✅ GUILD-ONLY: only visible in control server
    @app_commands.guilds(ADMIN_GUILD_OBJ)
    @app_commands.command(
        name="setfeedbackchannel",
        description="Set the current channel to receive feedback (Admin only in FoxCom)."
    )
    async def setfeedbackchannel(self, interaction: discord.Interaction):
        if await deny_if_blocked(interaction):
            return

        if interaction.guild is None or interaction.guild.id != ADMIN_SERVER_ID or not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Only administrators in the FoxCom server can use this.", ephemeral=True)
            return

        db.set_feedback_channel(interaction.channel.id)
        await interaction.response.send_message("✅ This channel is now set to receive feedback messages.", ephemeral=True)

    # Global: usable in approved servers
    @app_commands.command(name="feedback", description="Send feedback to the FoxCom team.")
    async def feedback(self, interaction: discord.Interaction, message: str):
        if await deny_if_blocked(interaction):
            return

        if not interaction.guild or not db.is_guild_approved(interaction.guild.id):
            await interaction.response.send_message("❌ This server is not approved to send feedback.", ephemeral=True)
            return

        if contains_disallowed_mentions(message):
            await interaction.response.send_message(
                "❌ Mentions are not allowed in feedback (no @everyone, @here, roles, user pings, or '@').",
                ephemeral=True
            )
            return

        channel_id = db.get_feedback_channel()
        if not channel_id:
            await interaction.response.send_message(
                "⚠️ Feedback channel is not set. Ask FoxCom admins to run `/setfeedbackchannel`.",
                ephemeral=True
            )
            return

        feedback_channel = self.bot.get_channel(channel_id)
        if not feedback_channel:
            await interaction.response.send_message(
                "❌ Could not find the feedback channel. Please re-set it using /setfeedbackchannel.",
                ephemeral=True
            )
            return

        embed = discord.Embed(title="📝 New Feedback Submission", description=message, color=discord.Color.blurple())
        embed.add_field(name="From", value=str(interaction.user), inline=False)
        embed.add_field(name="Server", value=f"{interaction.guild.name} (`{interaction.guild.id}`)", inline=False)
        embed.set_footer(text=f"Submitted {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

        await feedback_channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        await interaction.response.send_message("✅ Your feedback has been submitted. Thank you!", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(FeedbackCog(bot))

