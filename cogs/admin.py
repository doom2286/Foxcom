import discord
from discord.ext import commands
from discord import app_commands

from core import db
from core.config import load_config
from core.utils import utc_now_iso

CFG = load_config()
ADMIN_SERVER_ID = int(CFG.get("admin_server_id") or 0)

# Used to scope admin-only slash commands to the control server
ADMIN_GUILD_OBJ = discord.Object(id=ADMIN_SERVER_ID)


def _in_control_server(interaction: discord.Interaction) -> bool:
    return interaction.guild is not None and interaction.guild.id == ADMIN_SERVER_ID


def _is_admin(interaction: discord.Interaction) -> bool:
    return bool(getattr(interaction.user, "guild_permissions", None) and interaction.user.guild_permissions.administrator)


class _Pager(discord.ui.View):
    def __init__(self, make_embed_fn, total_items: int, per_page: int = 8):
        super().__init__(timeout=180)
        self.make_embed_fn = make_embed_fn
        self.per_page = max(1, int(per_page))
        self.page = 0
        self.total_items = int(total_items)
        self.max_page = max(0, (self.total_items - 1) // self.per_page)

        self.prev_button.disabled = True
        self.next_button.disabled = (self.max_page == 0)

    async def _refresh(self, interaction: discord.Interaction):
        self.prev_button.disabled = (self.page <= 0)
        self.next_button.disabled = (self.page >= self.max_page)
        await interaction.response.edit_message(embed=self.make_embed_fn(self.page, self.per_page), view=self)

    @discord.ui.button(label="◀ Prev", style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page > 0:
            self.page -= 1
        await self._refresh(interaction)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.page < self.max_page:
            self.page += 1
        await self._refresh(interaction)


class AdminCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # ---------------------------------------------
    # /foxcomlistservers  (live: bot.guilds cache)
    # Hidden from other servers via @guilds()
    # ---------------------------------------------
    @app_commands.command(name="foxcomlistservers", description="List ALL servers the bot is currently in (control server only).")
    @app_commands.guilds(ADMIN_GUILD_OBJ)
    @app_commands.default_permissions(administrator=True)
    async def foxcomlistservers(self, interaction: discord.Interaction):
        # Extra runtime guard (defense in depth)
        if not _in_control_server(interaction):
            await interaction.response.send_message("❌ Use this in the FoxCom control server.", ephemeral=True)
            return
        if not _is_admin(interaction):
            await interaction.response.send_message("❌ Admins only.", ephemeral=True)
            return

        guilds = sorted(list(self.bot.guilds), key=lambda g: (g.name or "").lower())
        total = len(guilds)

        def make_embed(page: int, per_page: int) -> discord.Embed:
            max_page = max(1, (total - 1) // per_page + 1)
            emb = discord.Embed(
                title="🌐 Bot Installed Servers",
                description=f"Page {page + 1}/{max_page} • Total: {total}",
                color=discord.Color.blurple(),
            )

            if total == 0:
                emb.add_field(name="(none)", value="The bot isn't in any servers.", inline=False)
                return emb

            start = page * per_page
            end = start + per_page
            for g in guilds[start:end]:
                emb.add_field(
                    name=g.name or "(unknown)",
                    value=f"ID: `{g.id}` • Members: `{getattr(g, 'member_count', 'N/A')}`",
                    inline=False,
                )
            return emb

        view = _Pager(make_embed, total_items=total, per_page=8)
        await interaction.response.send_message(embed=make_embed(0, view.per_page), view=view, ephemeral=True)

    # ---------------------------------------------
    # /foxcomlistapproved (DB: approved_servers)
    # Hidden from other servers via @guilds()
    # ---------------------------------------------
    @app_commands.command(name="foxcomlistapproved", description="List all approved (verified) servers (control server only).")
    @app_commands.guilds(ADMIN_GUILD_OBJ)
    @app_commands.default_permissions(administrator=True)
    async def foxcomlistapproved(self, interaction: discord.Interaction):
        if not _in_control_server(interaction):
            await interaction.response.send_message("❌ Use this in the FoxCom control server.", ephemeral=True)
            return
        if not _is_admin(interaction):
            await interaction.response.send_message("❌ Admins only.", ephemeral=True)
            return

        try:
            conn = db.connect()
            cur = conn.cursor()
            cur.execute(
                "SELECT guild_id, server_name, regiment, approved_at, approved_by "
                "FROM approved_servers ORDER BY approved_at DESC"
            )
            rows = cur.fetchall() or []
            conn.close()
        except Exception as e:
            await interaction.response.send_message(f"❌ DB error: {e}", ephemeral=True)
            return

        total = len(rows)

        def make_embed(page: int, per_page: int) -> discord.Embed:
            max_page = max(1, (total - 1) // per_page + 1)
            emb = discord.Embed(
                title="✅ Approved Servers",
                description=f"Page {page + 1}/{max_page} • Total: {total}",
                color=discord.Color.green(),
            )

            if total == 0:
                emb.add_field(name="(none)", value="No approved servers found.", inline=False)
                return emb

            start = page * per_page
            end = start + per_page
            for r in rows[start:end]:
                name = r["server_name"] or "(unknown)"
                gid = r["guild_id"]
                reg = (r["regiment"] or "").strip() or "N/A"
                approved_at = r["approved_at"] or "N/A"
                approved_by = r["approved_by"] or "N/A"
                emb.add_field(
                    name=f"{name}",
                    value=f"ID: `{gid}`\nRegiment: **{reg}**\nApproved: `{approved_at}`\nBy: `{approved_by}`",
                    inline=False,
                )
            return emb

        view = _Pager(make_embed, total_items=total, per_page=8)
        await interaction.response.send_message(embed=make_embed(0, view.per_page), view=view, ephemeral=True)

    # ---------------------------------------------
    # /foxcomrevoke <guild_id> [reason]
    # Hidden from other servers via @guilds()
    # ---------------------------------------------
    @app_commands.command(name="foxcomrevoke", description="Revoke a server's verification (control server only).")
    @app_commands.describe(guild_id="The server (guild) ID to revoke", reason="Optional reason for revocation")
    @app_commands.guilds(ADMIN_GUILD_OBJ)
    @app_commands.default_permissions(administrator=True)
    async def foxcomrevoke(self, interaction: discord.Interaction, guild_id: str, reason: str = ""):
        if not _in_control_server(interaction):
            await interaction.response.send_message("❌ Use this in the FoxCom control server.", ephemeral=True)
            return
        if not _is_admin(interaction):
            await interaction.response.send_message("❌ Admins only.", ephemeral=True)
            return

        try:
            gid = int(str(guild_id).strip())
        except Exception:
            await interaction.response.send_message("❌ Invalid guild_id. Provide a numeric server ID.", ephemeral=True)
            return

        # Look up stored server name (optional)
        server_name = None
        try:
            conn = db.connect()
            cur = conn.cursor()
            cur.execute("SELECT server_name FROM approved_servers WHERE guild_id=?", (int(gid),))
            r = cur.fetchone()
            if r:
                server_name = r["server_name"]
            conn.close()
        except Exception:
            pass

        # Revoke: delete from approved + clear channels
        try:
            conn = db.connect()
            cur = conn.cursor()
            cur.execute("DELETE FROM approved_servers WHERE guild_id=?", (int(gid),))
            removed_approved = cur.rowcount > 0
            cur.execute("DELETE FROM channels WHERE guild_id=?", (int(gid),))
            conn.commit()
            conn.close()
        except Exception as e:
            await interaction.response.send_message(f"❌ DB error while revoking: {e}", ephemeral=True)
            return

        if not removed_approved:
            await interaction.response.send_message("⚠️ That server was not approved (no changes made).", ephemeral=True)
            return

        # Log to the control server channel where the command ran
        try:
            log_embed = discord.Embed(
                title="⛔ Verification Revoked",
                color=discord.Color.red(),
                timestamp=discord.utils.utcnow(),
            )
            log_embed.add_field(name="Server", value=server_name or "(unknown)", inline=False)
            log_embed.add_field(name="Guild ID", value=f"`{gid}`", inline=False)
            log_embed.add_field(name="Revoked By", value=str(interaction.user), inline=False)
            log_embed.add_field(name="Reason", value=(reason or "").strip() or "N/A", inline=False)
            log_embed.set_footer(text=f"Revoked at {utc_now_iso()}")
            await interaction.channel.send(embed=log_embed)  # type: ignore[attr-defined]
        except Exception:
            pass

        await interaction.response.send_message(
            f"✅ Revoked verification for `{gid}`. (Also cleared its /foxcomchannelset channel.)",
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminCog(bot))
