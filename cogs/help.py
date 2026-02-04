# cogs/help.py
import discord
from discord.ext import commands
from discord import app_commands

from core.config import load_config

CFG = load_config()
ADMIN_SERVER_ID = int(CFG.get("admin_server_id") or 0)


def _flatten_commands(cmds: list[app_commands.Command | app_commands.Group]) -> list[app_commands.Command]:
    """Return a flat list of leaf commands (including subcommands)."""
    out: list[app_commands.Command] = []

    def walk(c):
        # Groups have .commands (subcommands)
        if isinstance(c, app_commands.Group):
            for sc in c.commands:
                walk(sc)
        else:
            out.append(c)

    for c in cmds:
        walk(c)
    return out


def _cmd_full_name(cmd: app_commands.Command) -> str:
    # For subcommands, qualified_name becomes "group sub"
    return cmd.qualified_name


class HelpCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="foxcomhelp",
        description="Show a list of FoxCom commands and what they do."
    )
    @app_commands.describe(scope="Where to list commands from: here | global | admin")
    async def foxcomhelp(self, interaction: discord.Interaction, scope: str = "here"):
        scope = (scope or "here").lower().strip()
        guild = interaction.guild

        # Decide which command set to show
        # - here  : commands registered for this guild (includes globals + guild-specific, if synced)
        # - global: global commands only
        # - admin : commands in the admin server (useful for staff)
        if scope == "global":
            cmds = self.bot.tree.get_commands(guild=None)
            title = "FoxCom Commands (Global)"
        elif scope == "admin":
            admin_obj = discord.Object(id=ADMIN_SERVER_ID) if ADMIN_SERVER_ID else None
            cmds = self.bot.tree.get_commands(guild=admin_obj) if admin_obj else []
            title = "FoxCom Commands (Admin Server)"
        else:
            # default: "here"
            if not guild:
                cmds = self.bot.tree.get_commands(guild=None)
                title = "FoxCom Commands (Global)"
            else:
                cmds = self.bot.tree.get_commands(guild=discord.Object(id=guild.id))
                title = f"FoxCom Commands ({guild.name})"

        flat = _flatten_commands(list(cmds))

        # Sort by name for stable output
        flat.sort(key=lambda c: _cmd_full_name(c))

        if not flat:
            await interaction.response.send_message(
                "⚠️ No commands found for that scope. If you just added commands, try again after a sync/restart.",
                ephemeral=True
            )
            return

        # Build embed (Discord embed field limits apply, so we chunk)
        embed = discord.Embed(title=title, color=discord.Color.blurple())
        embed.set_footer(text="Tip: /foxcomhelp scope: here | global | admin")

        # Each field value max is 1024 chars; chunk into pages
        chunk: list[str] = []
        current_len = 0

        def flush():
            nonlocal chunk, current_len
            if chunk:
                embed.add_field(name="Commands", value="\n".join(chunk), inline=False)
                chunk = []
                current_len = 0

        for c in flat:
            name = "/" + _cmd_full_name(c)
            desc = (c.description or "No description.").strip()
            line = f"**{name}** — {desc}"

            # Keep fields within safe size
            if current_len + len(line) + 1 > 900:
                flush()

            chunk.append(line)
            current_len += len(line) + 1

        flush()

        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(HelpCog(bot))
