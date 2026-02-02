import discord
from discord.ext import commands
from discord import app_commands
from core import db
from core.config import load_config

CFG = load_config()
ADMIN_SERVER_ID = int(CFG.get("admin_server_id") or 0)
ADMIN_GUILD_OBJ = discord.Object(id=ADMIN_SERVER_ID)


async def deny_if_blocked(interaction: discord.Interaction) -> bool:
    # Allow control-server admins to bypass blocks
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


class AdminCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    def _guard_admin(self, interaction: discord.Interaction) -> bool:
        if interaction.guild is None or interaction.guild.id != ADMIN_SERVER_ID:
            return False
        return bool(getattr(interaction.user, "guild_permissions", None) and interaction.user.guild_permissions.administrator)

    @app_commands.guilds(ADMIN_GUILD_OBJ)
    @app_commands.command(
        name="aprovedregi",
        description="List all approved regiments and server info (Admin only in FoxCom)."
    )
    async def aprovedregi(self, interaction: discord.Interaction):
        if await deny_if_blocked(interaction):
            return
        if not self._guard_admin(interaction):
            await interaction.response.send_message("❌ Admins only in FoxCom control server.", ephemeral=True)
            return

        rows = db.list_approved()
        if not rows:
            await interaction.response.send_message("⚠️ No approved servers found.", ephemeral=True)
            return

        embed = discord.Embed(title="📋 Approved Regiments", color=discord.Color.green())
        for r in rows:
            server_id = str(r["guild_id"])
            regiment = r["regiment"] or "Unknown"
            server_name = r["server_name"] or "Unknown Server"
            approved_by = r["approved_by"] or "Unknown"
            approved_at = r["approved_at"] or "Unknown"

            g = self.bot.get_guild(int(server_id))
            live_name = g.name if g else server_name

            embed.add_field(
                name=f"{regiment}",
                value=(
                    f"**Server:** {live_name}\n"
                    f"**Server ID:** `{server_id}`\n"
                    f"**Approved By:** {approved_by}\n"
                    f"**Approved At:** {approved_at}"
                ),
                inline=False
            )

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.guilds(ADMIN_GUILD_OBJ)
    @app_commands.command(
        name="clearapproved",
        description="Clear all approved regiments (Admin only in FoxCom)."
    )
    async def clearapproved(self, interaction: discord.Interaction):
        if await deny_if_blocked(interaction):
            return
        if not self._guard_admin(interaction):
            await interaction.response.send_message("❌ Admins only in FoxCom control server.", ephemeral=True)
            return

        class ConfirmClear(discord.ui.View):
            def __init__(self, caller_id: int):
                super().__init__(timeout=30)
                self.caller_id = caller_id
                self.value = None

            async def _guard(self, i: discord.Interaction) -> bool:
                if i.user.id != self.caller_id:
                    await i.response.send_message("❌ You can't confirm someone else's command.", ephemeral=True)
                    return False
                return True

            @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger)
            async def confirm(self, i: discord.Interaction, button: discord.ui.Button):
                if not await self._guard(i):
                    return
                self.value = True
                await i.response.defer(ephemeral=True)
                self.stop()

            @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
            async def cancel(self, i: discord.Interaction, button: discord.ui.Button):
                if not await self._guard(i):
                    return
                self.value = False
                await i.response.defer(ephemeral=True)
                self.stop()

        view = ConfirmClear(caller_id=interaction.user.id)
        await interaction.response.send_message(
            "⚠️ Are you sure you want to clear **all approved regiments**? This cannot be undone.",
            ephemeral=True,
            view=view
        )
        await view.wait()

        if view.value:
            db.clear_approved()
            await interaction.followup.send("✅ All approved regiments have been cleared.", ephemeral=True)
        else:
            await interaction.followup.send("❌ Operation cancelled.", ephemeral=True)

    @app_commands.guilds(ADMIN_GUILD_OBJ)
    @app_commands.command(
        name="blockuser",
        description="Block a user from using FoxCom commands (Admin only in FoxCom)."
    )
    @app_commands.describe(user="The user to block", reason="Optional reason")
    async def blockuser(self, interaction: discord.Interaction, user: discord.User, reason: str = ""):
        if await deny_if_blocked(interaction):
            return
        if not self._guard_admin(interaction):
            await interaction.response.send_message("❌ Admins only in FoxCom control server.", ephemeral=True)
            return

        db.block_user(user.id, str(user), str(interaction.user), reason)
        msg = f"✅ Blocked **{user}** (`{user.id}`) from using FoxCom commands."
        if reason.strip():
            msg += f"\n📝 Reason: {reason.strip()}"
        await interaction.response.send_message(msg, ephemeral=True)

    @app_commands.guilds(ADMIN_GUILD_OBJ)
    @app_commands.command(
        name="unblockuser",
        description="Unblock a user from using FoxCom commands (Admin only in FoxCom)."
    )
    @app_commands.describe(user="The user to unblock")
    async def unblockuser(self, interaction: discord.Interaction, user: discord.User):
        if await deny_if_blocked(interaction):
            return
        if not self._guard_admin(interaction):
            await interaction.response.send_message("❌ Admins only in FoxCom control server.", ephemeral=True)
            return

        removed = db.unblock_user(user.id)

        if removed:
            await interaction.response.send_message(
                f"✅ Unblocked **{user}** (`{user.id}`).",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                f"ℹ️ **{user}** (`{user.id}`) was not blocked.",
                ephemeral=True
            )

    @app_commands.guilds(ADMIN_GUILD_OBJ)
    @app_commands.command(
        name="setuserrep",
        description="Set a user's reputation (Admin only in FoxCom)."
    )
    @app_commands.describe(user="The user to set rep for", rep="The rep value to set")
    async def setuserrep(self, interaction: discord.Interaction, user: discord.User, rep: int):
        if await deny_if_blocked(interaction):
            return
        if not self._guard_admin(interaction):
            await interaction.response.send_message("❌ Admins only in FoxCom control server.", ephemeral=True)
            return

        # Optional sanity limits (adjust/remove if you want)
        if rep < -100000 or rep > 100000:
            await interaction.response.send_message("❌ Rep value out of range.", ephemeral=True)
            return

        # Requires db.set_user_rep(...) implemented in core/db.py
        db.set_user_rep(user.id, str(user), rep, str(interaction.user))

        await interaction.response.send_message(
            f"✅ Set rep for **{user}** (`{user.id}`) to **{rep}**.",
            ephemeral=True
        )

    @app_commands.guilds(ADMIN_GUILD_OBJ)
    @app_commands.command(
        name="dbstatus",
        description="Show database row counts + last prune time (Admin only in FoxCom)."
    )
    async def dbstatus(self, interaction: discord.Interaction):
        if await deny_if_blocked(interaction):
            return
        if not self._guard_admin(interaction):
            await interaction.response.send_message("❌ Admins only in FoxCom control server.", ephemeral=True)
            return

        counts = db.counts()
        last_prune = db.get_last_prune()

        embed = discord.Embed(title="🗄️ FoxCom DB Status", color=discord.Color.dark_grey())
        embed.add_field(name="Last Prune (UTC)", value=(last_prune or "Never"), inline=False)

        embed.add_field(
            name="Core Tables",
            value=(
                f"channels: **{counts['channels']}**\n"
                f"approved_servers: **{counts['approved_servers']}**\n"
                f"pending_requests: **{counts['pending_requests']}**\n"
                f"feedback_config: **{counts['feedback_config']}**\n"
                f"banlist: **{counts['banlist']}**"
            ),
            inline=False
        )

        embed.add_field(
            name="Reputation Tables",
            value=(
                f"rep_users: **{counts['rep_users']}**\n"
                f"rep_messages (<=24h): **{counts['rep_messages']}**\n"
                f"rep_votes: **{counts['rep_votes']}**"
            ),
            inline=False
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminCog(bot))

