import math
import discord
from discord.ext import commands
from discord import app_commands
from core import db
from core.config import load_config

CFG = load_config()
ADMIN_SERVER_ID = int(CFG.get("admin_server_id") or 0)

THUMBS_UP = "👍"
THUMBS_DOWN = "👎"

# ----- logarithmic tier scaling -----
TIER_NAMES = ["Recruit", "Regular", "Trusted", "Veteran", "Elite", "Legend"]

# Tuned so Regular ≈ 10 rep, Legend ≈ 250 rep (and grows harder each tier)
LOG_BASE = 1.8667452839806598
LOG_SCALE = 10 / (LOG_BASE - 1)  # ≈ 11.5374


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


def rep_threshold(level: int) -> int:
    """Rep required to reach a given tier level."""
    level = max(0, int(level))
    return max(0, int(round(LOG_SCALE * (LOG_BASE ** level - 1))))


def rep_level(rep: int) -> int:
    """Map raw rep -> tier level using a logarithmic curve."""
    rep = int(rep)
    if rep <= 0:
        return 0
    lvl = int(math.floor(math.log(rep / LOG_SCALE + 1.0, LOG_BASE)))
    return max(0, min(lvl, len(TIER_NAMES) - 1))


def rep_stars(rep: int) -> str:
    # Stars based on tier level (cap at 3)
    lvl = rep_level(rep)
    return "★" * min(3, lvl)


def rep_milestone(rep: int):
    lvl = rep_level(rep)
    current = TIER_NAMES[lvl]

    if lvl >= len(TIER_NAMES) - 1:
        return current, None, None

    next_name = TIER_NAMES[lvl + 1]
    next_at = rep_threshold(lvl + 1)
    return current, next_name, next_at


class ReputationCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # ---- reaction listeners ----
    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if self.bot.user and payload.user_id == self.bot.user.id:
            return

        emoji = str(payload.emoji)
        if emoji not in (THUMBS_UP, THUMBS_DOWN):
            return

        msg = db.get_rep_message(payload.message_id)
        if not msg:
            return

        # only within 4h window (REP_TTL_SECONDS should be 4*60*60 in db.py)
        if not db.within_rep_window(msg["created_at"]):
            db.delete_rep_message(payload.message_id)
            db.prune_rep()
            return

        author_id = int(msg["author_id"])
        if payload.user_id == author_id:
            return  # no self-votes

        new_vote = 1 if emoji == THUMBS_UP else -1
        old_vote = db.get_vote(payload.message_id, payload.user_id)

        if old_vote == new_vote:
            return

        delta = new_vote - (old_vote or 0)
        db.set_vote(payload.message_id, payload.user_id, new_vote)
        db.adjust_rep(author_id, delta)

        db.prune_rep()

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        emoji = str(payload.emoji)
        if emoji not in (THUMBS_UP, THUMBS_DOWN):
            return

        msg = db.get_rep_message(payload.message_id)
        if not msg:
            return

        # only within 4h window (REP_TTL_SECONDS should be 4*60*60 in db.py)
        if not db.within_rep_window(msg["created_at"]):
            db.delete_rep_message(payload.message_id)
            db.prune_rep()
            return

        removed_vote = 1 if emoji == THUMBS_UP else -1
        old_vote = db.get_vote(payload.message_id, payload.user_id)
        if old_vote != removed_vote:
            return

        author_id = int(msg["author_id"])
        db.delete_vote(payload.message_id, payload.user_id)
        db.adjust_rep(author_id, -removed_vote)

        db.prune_rep()

    # ---- commands ----
    @app_commands.command(name="rep", description="Show reputation for a user (or yourself).")
    @app_commands.describe(user="User to check (optional)")
    async def rep(self, interaction: discord.Interaction, user: discord.User | None = None):
        if await deny_if_blocked(interaction):
            return

        if not interaction.guild or not db.is_guild_approved(interaction.guild.id):
            await interaction.response.send_message("❌ This server is not approved to use FoxCom.", ephemeral=True)
            return

        target = user or interaction.user
        rep_value = db.get_rep(target.id)

        stars = rep_stars(rep_value)
        tier, next_name, next_at = rep_milestone(rep_value)

        embed = discord.Embed(title="📈 FoxCom Reputation", color=discord.Color.blurple())
        embed.add_field(name="User", value=(f"{stars} {target}" if stars else str(target)), inline=False)
        embed.add_field(name="Reputation", value=str(rep_value), inline=True)
        embed.add_field(name="Tier", value=tier, inline=True)

        if next_at is not None:
            remaining = max(0, next_at - int(rep_value))
            embed.add_field(
                name="Next",
                value=f"{next_name} at **{next_at}** (need **{remaining}** more)",
                inline=True,
            )
        else:
            embed.add_field(name="Next", value="🏆 Max tier reached", inline=True)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="toprep", description="Show the top reputation users.")
    @app_commands.describe(limit="How many to show (max 25)")
    async def toprep(self, interaction: discord.Interaction, limit: int = 10):
        if await deny_if_blocked(interaction):
            return

        if not interaction.guild or not db.is_guild_approved(interaction.guild.id):
            await interaction.response.send_message("❌ This server is not approved to use FoxCom.", ephemeral=True)
            return

        limit = max(1, min(25, int(limit)))
        rows = db.leaderboard(limit)

        if not rows:
            await interaction.response.send_message("⚠️ No reputation data yet.", ephemeral=True)
            return

        embed = discord.Embed(title=f"🏅 Reputation Leaderboard (Top {len(rows)})", color=discord.Color.gold())
        lines = []
        for idx, r in enumerate(rows, start=1):
            rep_value = int(r["rep"])
            name = r["user_name"] or f"User {r['user_id']}"
            stars = rep_stars(rep_value)
            lines.append(f"**{idx}.** {(stars + ' ' if stars else '')}{name} — **{rep_value}**")

        embed.description = "\n".join(lines)
        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(ReputationCog(bot))

