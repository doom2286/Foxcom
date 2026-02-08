# cogs/help.py
import discord
from discord.ext import commands
from discord import app_commands

from core.config import load_config

CFG = load_config()
ADMIN_SERVER_ID = int(CFG.get("admin_server_id") or 0)

# -------------------- Categorization (module-level) --------------------
CATEGORIES: dict[str, dict[str, object]] = {
    "overview": {
        "title": "FoxCom Help",
        "blurb": (
            "Use the **Category** picker to see commands by feature.\n\n"
            "Common flow: **/foxcomchannelset** → **/foxcomverify** → (after approval) broadcast/group commands."
        ),
        "cmds": set(),
    },
    "setup": {
        "title": "Setup",
        "blurb": "Getting your server ready for FoxCom.",
        "cmds": {"foxcomchannelset", "foxcomverify"},
    },
    "broadcast": {
        "title": "Broadcasts",
        "blurb": "Cross-server alerts (approved servers only).",
        "cmds": {"qrf", "logi", "battle", "foxcomtest", "foxcomreport"},
    },
    "groups": {
        "title": "Groups",
        "blurb": "Private/public server groups and group-only broadcasts.",
        "cmds": {
            "creategroup",
            "joingroup",
            "leavegroup",
            "listgroup",
            "groupqrf",
            "groupbattle",
            "grouplogi",
            "grouptranslate",
            "groupglobaltranslate",
            "makegrouplead",
            "delgroup",
            "listmembers",
            "removemembers",
        },
    },
    "translation": {
        "title": "Translation",
        "blurb": "Commands related to auto-translation toggles.",
        "cmds": {"toggletranslation", "translationstatus", "grouptranslate", "groupglobaltranslate"},
    },
    "reputation": {
        "title": "Reputation",
        "blurb": "Rep / leaderboard.",
        "cmds": {"rep", "toprep"},
    },
    "feedback": {
        "title": "Feedback",
        "blurb": "Send feedback to FoxCom staff.",
        "cmds": {"feedback", "setfeedbackchannel"},
    },
    "admin": {
        "title": "Admin (Control Server)",
        "blurb": "Control-server administration commands.",
        "cmds": {
            "aprovedregi",
            "clearapproved",
            "blockuser",
            "unblockuser",
            "setuserrep",
            "toggletranslation",
            "translationstatus",
            "dbstatus",
            "setfeedbackchannel",
        },
    },
}


def make_category_choices() -> list[app_commands.Choice[str]]:
    order = ["overview", "setup", "broadcast", "groups", "translation", "reputation", "feedback", "admin"]
    out: list[app_commands.Choice[str]] = []
    for key in order:
        info = CATEGORIES.get(key)
        if not info:
            continue
        title = str(info.get("title") or key.title())
        out.append(app_commands.Choice(name=title, value=key))
    return out


def flatten_commands(cmds: list[app_commands.Command | app_commands.Group]) -> list[app_commands.Command]:
    """Return a flat list of leaf commands (including subcommands)."""
    out: list[app_commands.Command] = []

    def walk(c):
        if isinstance(c, app_commands.Group):
            for sc in c.commands:
                walk(sc)
        else:
            out.append(c)

    for c in cmds:
        walk(c)

    return out


def cmd_full_name(cmd: app_commands.Command) -> str:
    return cmd.qualified_name


def primary_name(cmd: app_commands.Command) -> str:
    return (cmd.qualified_name or "").split(" ", 1)[0].strip().lower()


def merge_unique_commands(
    a: list[app_commands.Command | app_commands.Group],
    b: list[app_commands.Command | app_commands.Group],
) -> list[app_commands.Command | app_commands.Group]:
    """Merge commands and dedupe by qualified_name (or name)."""
    merged: list[app_commands.Command | app_commands.Group] = []
    seen: set[str] = set()

    for lst in (a, b):
        for c in lst:
            qn = getattr(c, "qualified_name", None) or getattr(c, "name", "")
            if qn in seen:
                continue
            seen.add(qn)
            merged.append(c)

    return merged


class HelpCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="foxcomhelp",
        description="Show a list of FoxCom commands and what they do.",
    )
    @app_commands.describe(category="Pick a category to filter commands")
    @app_commands.choices(category=make_category_choices())
    async def foxcomhelp(
        self,
        interaction: discord.Interaction,
        category: app_commands.Choice[str] | None = None,
    ):
        cat_val = (category.value if category else "overview").lower().strip()

        # In a guild, include BOTH global + guild commands
        global_cmds = list(self.bot.tree.get_commands(guild=None))

        if interaction.guild:
            guild_cmds = list(
                self.bot.tree.get_commands(guild=discord.Object(id=interaction.guild.id))
            )
            cmds = merge_unique_commands(global_cmds, guild_cmds)
            title = f"FoxCom Commands ({interaction.guild.name})"
        else:
            cmds = global_cmds
            title = "FoxCom Commands (Global)"

        flat = flatten_commands(list(cmds))

        cat = CATEGORIES.get(cat_val, CATEGORIES["overview"])
        wanted = set(cat.get("cmds") or set())

        if cat_val != "overview":
            flat = [c for c in flat if primary_name(c) in wanted]

        flat.sort(key=lambda c: cmd_full_name(c))

        if not flat:
            await interaction.response.send_message(
                "⚠️ No commands found for that category here. (Commands may not be synced yet.)",
                ephemeral=True,
            )
            return

        cat_title = str(cat.get("title") or cat_val.title())
        embed = discord.Embed(
            title=f"{cat_title} — {title}",
            color=discord.Color.blurple(),
        )

        blurb = (cat.get("blurb") or "").strip()
        if blurb:
            embed.description = blurb

        if cat_val == "overview":
            lines = []
            for ch in make_category_choices():
                if ch.value == "overview":
                    continue
                lines.append(f"• **{ch.name}**")
            embed.add_field(
                name="Categories",
                value="\n".join(lines) if lines else "(none)",
                inline=False,
            )

        embed.set_footer(text="Tip: /foxcomhelp → pick Category")

        # Keep command output under embed field limits
        chunk: list[str] = []
        current_len = 0

        def flush():
            nonlocal chunk, current_len
            if chunk:
                embed.add_field(name="Commands", value="\n".join(chunk), inline=False)
                chunk = []
                current_len = 0

        for c in flat:
            name = "/" + cmd_full_name(c)
            desc = (c.description or "No description.").strip()
            line = f"**{name}** — {desc}"

            if current_len + len(line) + 1 > 900:
                flush()

            chunk.append(line)
            current_len += len(line) + 1

        flush()

        await interaction.response.send_message(
            embed=embed,
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(HelpCog(bot))
