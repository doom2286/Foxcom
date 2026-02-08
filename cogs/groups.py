# cogs/groups.py
from __future__ import annotations

import re
import time
from typing import Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from core import db  # uses your sqlite tables + approval system

from core.config import load_config

# Translation helper; fall back to no-op if missing
try:
    from core.translate import maybe_translate_to_english  # type: ignore
except Exception:
    async def maybe_translate_to_english(api_key: str, text: str, *, enabled: bool = True, min_chars: int = 12):
        return text, None, False, None

CFG = load_config()
GOOGLE_TRANSLATE_API_KEY = (CFG.get("google_translate_api_key") or "").strip()
TRANSLATE_ENABLED = str(CFG.get("translate_enabled") or "true").strip().lower() in ("1","true","yes","y","on")
TRANSLATE_MIN_CHARS = int(CFG.get("translate_min_chars") or 12)
TRANSLATE_SHOW_ORIGINAL = str(CFG.get("translate_show_original") or "true").strip().lower() in ("1","true","yes","y","on")


# Rate limit: seconds per (group_id, user_id) for group broadcast commands
GROUP_BROADCAST_COOLDOWN = 60


def _now_ts() -> int:
    return int(time.time())


def _sanitize_broadcast_text(text: str) -> str:
    """
    Prevent ping abuse: neutralize @everyone/@here and replace mention syntaxes.
    """
    if not text:
        return ""
    text = text.replace("@everyone", "@\u200beveryone").replace("@here", "@\u200bhere")
    text = re.sub(r"<@&\d+>", "[role]", text)      # role mention
    text = re.sub(r"<@!?\\d+>", "[user]", text)     # user mention
    text = re.sub(r"<#\d+>", "[channel]", text)    # channel mention
    return text


def _get_broadcast_channel_id_for_guild(guild_id: int) -> Optional[int]:
    """
    Uses your DB table 'channels' (guild_id -> channel_id) from /foxcomchannelset.
    """
    conn = db.connect()
    cur = conn.cursor()
    cur.execute("SELECT channel_id FROM channels WHERE guild_id=?", (int(guild_id),))
    r = cur.fetchone()
    conn.close()
    return int(r["channel_id"]) if r and r["channel_id"] else None


class ConfirmDeleteView(discord.ui.View):
    def __init__(self, requester_id: int, timeout: float = 30.0):
        super().__init__(timeout=timeout)
        self.requester_id = requester_id
        self.confirmed: Optional[bool] = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the requester can use these buttons.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm delete", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = True
        self.stop()
        await interaction.response.edit_message(content="✅ Confirmed. Deleting…", view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = False
        self.stop()
        await interaction.response.edit_message(content="❎ Cancelled.", view=None)


class Groups(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        db.init()  # ensure tables exist

        # (group_id, user_id) -> last_ts
        self._cooldowns: dict[tuple[int, int], int] = {}

    # ---------- Common checks ----------
    async def _require_guild(self, interaction: discord.Interaction) -> bool:
        if interaction.guild is None:
            await interaction.response.send_message("This command can only be used in a server.", ephemeral=True)
            return False
        return True

    async def _require_approved(self, interaction: discord.Interaction) -> bool:
        if interaction.guild and not db.is_guild_approved(interaction.guild.id):
            await interaction.response.send_message(
                "This server is not verified/approved to use FoxCom commands.",
                ephemeral=True
            )
            return False
        return True

    def _cooldown_ok(self, group_id: int, user_id: int) -> Tuple[bool, int]:
        now = _now_ts()
        key = (group_id, user_id)
        last = self._cooldowns.get(key, 0)
        if now - last < GROUP_BROADCAST_COOLDOWN:
            return False, GROUP_BROADCAST_COOLDOWN - (now - last)
        self._cooldowns[key] = now
        return True, 0

    # =========================
    # /creategroup
    # =========================
    @app_commands.command(name="creategroup", description="Create a new group (requires 25+ reputation).")
    @app_commands.describe(
        name="Group name",
        visibility="public or private",
        password="Password (required if private)"
    )
    @app_commands.choices(visibility=[
        app_commands.Choice(name="public", value="public"),
        app_commands.Choice(name="private", value="private"),
    ])
    async def creategroup(
        self,
        interaction: discord.Interaction,
        name: str,
        visibility: app_commands.Choice[str],
        password: str = ""  # <-- always shows in Discord UI
    ):
        if not await self._require_guild(interaction):
            return
        if not await self._require_approved(interaction):
            return

        rep = int(db.get_rep(interaction.user.id))
        if rep < 25:
            await interaction.response.send_message("You need **25+ reputation** to create a group.", ephemeral=True)
            return

        name = name.strip()
        if len(name) < 2 or len(name) > 40:
            await interaction.response.send_message("Group name must be between 2 and 40 characters.", ephemeral=True)
            return

        vis = visibility.value
        pw = password.strip()

        if vis == "private" and len(pw) < 3:
            await interaction.response.send_message("Private groups require a password (min 3 characters).", ephemeral=True)
            return

        try:
            group_id = db.create_group(
                name=name,
                visibility=vis,
                password=pw if pw else None,
                owner_user_id=interaction.user.id,
                guild_id=interaction.guild.id,
                guild_name=interaction.guild.name
            )
        except Exception as e:
            # Includes duplicate-name integrity errors
            msg = str(e)
            if "UNIQUE" in msg.upper():
                msg = "That group name already exists."
            await interaction.response.send_message(f"Failed to create group: {msg}", ephemeral=True)
            return

        await interaction.response.send_message(
            f"✅ Group **{name}** created ({vis}). This server has been joined automatically.\n"
            f"Group ID: `{group_id}`",
            ephemeral=True
        )

    # =========================
    # /joingroup
    # =========================
    @app_commands.command(name="joingroup", description="Join a group (server membership).")
    @app_commands.describe(name="Group name", password="Password (required if the group is private)")
    async def joingroup(self, interaction: discord.Interaction, name: str, password: str = ""):
        if not await self._require_guild(interaction):
            return
        if not await self._require_approved(interaction):
            return

        name = name.strip()
        group_id = db.get_group_id(name)
        if not group_id:
            await interaction.response.send_message("Group not found.", ephemeral=True)
            return

        if not db.check_group_password(group_id, password.strip() or None):
            await interaction.response.send_message("Incorrect password (or password required).", ephemeral=True)
            return

        db.join_group(group_id, interaction.guild.id, interaction.guild.name)
        await interaction.response.send_message(f"✅ This server joined **{name}**.", ephemeral=True)

    # =========================
    # /leavegroup
    # =========================
    @app_commands.command(name="leavegroup", description="Leave a group (server membership).")
    @app_commands.describe(name="Group name")
    async def leavegroup(self, interaction: discord.Interaction, name: str):
        if not await self._require_guild(interaction):
            return
        if not await self._require_approved(interaction):
            return

        name = name.strip()
        group_id = db.get_group_id(name)
        if not group_id:
            await interaction.response.send_message("Group not found.", ephemeral=True)
            return

        removed = db.leave_group(group_id, interaction.guild.id)
        if removed:
            await interaction.response.send_message(f"✅ This server left **{name}**.", ephemeral=True)
        else:
            await interaction.response.send_message("This server is not in that group.", ephemeral=True)

    # =========================
    # /listgroup
    # =========================
    @app_commands.command(name="listgroup", description="List all groups this server is in.")
    async def listgroup(self, interaction: discord.Interaction):
        if not await self._require_guild(interaction):
            return
        if not await self._require_approved(interaction):
            return

        rows = db.list_groups_for_guild(interaction.guild.id)
        if not rows:
            await interaction.response.send_message("This server is not in any groups.", ephemeral=True)
            return

        lines = [f"• **{r['name']}** ({r['visibility']})" for r in rows]
        embed = discord.Embed(title="Groups (this server)", description="\n".join(lines))
        await interaction.response.send_message(embed=embed, ephemeral=True)


    # =========================
    # /publicgroups
    # =========================
    @app_commands.command(name="publicgroups", description="Browse groups that are marked public.")
    @app_commands.describe(search="Optional name search", page="Page number (starts at 1)")
    async def publicgroups(self, interaction: discord.Interaction, search: str = "", page: int = 1):
        if not await self._require_guild(interaction):
            return
        if not await self._require_approved(interaction):
            return

        page = max(1, int(page or 1))
        search = (search or "").strip()

        PAGE_SIZE = 10
        offset = (page - 1) * PAGE_SIZE

        conn = db.connect()
        cur = conn.cursor()

        # Total count for pagination
        if search:
            like = f"%{search}%"
            cur.execute(
                "SELECT COUNT(*) AS c FROM groups WHERE visibility='public' AND name LIKE ?",
                (like,),
            )
        else:
            cur.execute("SELECT COUNT(*) AS c FROM groups WHERE visibility='public'")
        total = int(cur.fetchone()["c"] or 0)

        if total == 0:
            conn.close()
            await interaction.response.send_message("No public groups found.", ephemeral=True)
            return

        # Page data with population counts (servers + users)
        where_sql = "g.visibility='public'"
        params = []
        if search:
            where_sql += " AND g.name LIKE ?"
            params.append(f"%{search}%")

        cur.execute(
            f"""
            SELECT
                g.group_id,
                g.name,
                g.created_at,
                COUNT(DISTINCT gs.guild_id) AS server_count,
                COUNT(DISTINCT gur.user_id) AS member_count
            FROM groups g
            LEFT JOIN group_servers gs ON gs.group_id = g.group_id
            LEFT JOIN group_user_roles gur ON gur.group_id = g.group_id
            WHERE {where_sql}
            GROUP BY g.group_id
            ORDER BY g.name COLLATE NOCASE ASC
            LIMIT ? OFFSET ?
            """,
            (*params, PAGE_SIZE, offset),
        )
        rows = cur.fetchall()
        conn.close()

        if not rows:
            await interaction.response.send_message("No results on that page.", ephemeral=True)
            return

        max_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
        page = min(page, max_pages)

        title = "Public Groups"
        if search:
            title += f" — search: {search}"

        embed = discord.Embed(title=title)
        embed.set_footer(text=f"Page {page}/{max_pages} • Total: {total}")

        # Embed field limits are tight; keep each entry short.
        lines = []
        for r in rows:
            gid = int(r["group_id"])
            name = str(r["name"])
            servers = int(r["server_count"] or 0)
            members = int(r["member_count"] or 0)
            lines.append(f"• **{name}** — {servers} servers • {members} members • ID `{gid}`")

        embed.description = "\n".join(lines)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    # =========================
    # =========================
    # /grouptranslate (user setting)
    # =========================
    @app_commands.command(name="grouptranslate", description="Toggle auto-translation for YOUR group broadcasts.")
    async def grouptranslate(self, interaction: discord.Interaction):
        if not await self._require_guild(interaction):
            return
        if not await self._require_approved(interaction):
            return

        new_state = db.toggle_user_group_translate_enabled(
            interaction.user.id,
            str(interaction.user),
            default=True,
        )
        state_txt = "ON" if new_state else "OFF"
        await interaction.response.send_message(
            f"🌐 Your group-message translation is now **{state_txt}**.",
            ephemeral=True,
        )

    # =========================
    # /groupglobaltranslate (owner/leader)
    # =========================
    @app_commands.command(name="groupglobaltranslate", description="Toggle translation for everyone in a group (owner/leader).")
    @app_commands.describe(groupname="Group name")
    async def groupglobaltranslate(self, interaction: discord.Interaction, groupname: str):
        if not await self._require_guild(interaction):
            return
        if not await self._require_approved(interaction):
            return

        groupname = groupname.strip()
        group_id = db.get_group_id(groupname)
        if not group_id:
            await interaction.response.send_message("Group not found.", ephemeral=True)
            return

        # Only allow toggling from a server that is actually in the group
        if not db.guild_in_group(group_id, interaction.guild.id):
            await interaction.response.send_message("This server is not in that group.", ephemeral=True)
            return

        role = db.get_user_group_role(group_id, interaction.user.id)
        if role not in ("owner", "leader"):
            await interaction.response.send_message(
                "You must be a **group leader** (or owner) to do that.",
                ephemeral=True,
            )
            return

        new_state = db.toggle_group_global_translate_enabled(group_id, default=True)
        state_txt = "ENABLED" if new_state else "DISABLED"
        await interaction.response.send_message(
            f"🌐 Group translation is now **{state_txt}** for **{groupname}**.",
            ephemeral=True,
        )

    # /makegrouplead
    # =========================
    @app_commands.command(name="makegrouplead", description="Promote a user to group leader.")
    @app_commands.describe(groupname="Group name", user="User to promote")
    async def makegrouplead(self, interaction: discord.Interaction, groupname: str, user: discord.User):
        if not await self._require_guild(interaction):
            return
        if not await self._require_approved(interaction):
            return

        groupname = groupname.strip()
        group_id = db.get_group_id(groupname)
        if not group_id:
            await interaction.response.send_message("Group not found.", ephemeral=True)
            return

        role = db.get_user_group_role(group_id, interaction.user.id)
        if role not in ("owner", "leader"):
            await interaction.response.send_message("You must be a **group leader** (or owner) to do that.", ephemeral=True)
            return

        db.set_user_group_role(group_id, user.id, "leader")
        await interaction.response.send_message(f"✅ **{user}** is now a leader of **{groupname}**.", ephemeral=True)

    # =========================
    # /delgroup
    # =========================
    @app_commands.command(name="delgroup", description="Delete a group (owner only).")
    @app_commands.describe(groupname="Group name")
    async def delgroup(self, interaction: discord.Interaction, groupname: str):
        if not await self._require_guild(interaction):
            return
        if not await self._require_approved(interaction):
            return

        groupname = groupname.strip()
        g = db.get_group_by_name(groupname)
        if not g:
            await interaction.response.send_message("Group not found.", ephemeral=True)
            return

        if int(g["owner_user_id"]) != interaction.user.id:
            await interaction.response.send_message("Only the **group owner** can delete the group.", ephemeral=True)
            return

        view = ConfirmDeleteView(requester_id=interaction.user.id)
        await interaction.response.send_message(
            f"⚠️ Are you sure you want to delete **{groupname}**?\nThis cannot be undone.",
            view=view,
            ephemeral=True
        )

        await view.wait()
        if view.confirmed:
            db.delete_group(int(g["group_id"]))

    # =========================
    # /listmembers
    # =========================
    @app_commands.command(name="listmembers", description="List servers linked to a group (by name).")
    @app_commands.describe(groupname="Group name")
    async def listmembers(self, interaction: discord.Interaction, groupname: str):
        if not await self._require_guild(interaction):
            return
        if not await self._require_approved(interaction):
            return

        groupname = groupname.strip()
        group_id = db.get_group_id(groupname)
        if not group_id:
            await interaction.response.send_message("Group not found.", ephemeral=True)
            return

        rows = db.list_servers_in_group(group_id)
        if not rows:
            await interaction.response.send_message("No servers are linked to this group.", ephemeral=True)
            return

        lines = [f"• {r['guild_name']}" for r in rows]
        embed = discord.Embed(title=f"Servers in {groupname}", description="\n".join(lines))
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # =========================
    # /removemembers
    # =========================
    @app_commands.command(name="removemembers", description="Remove a server from a group (leader/owner).")
    @app_commands.describe(groupname="Group name", guild_id="Server ID to remove (use /listmembers first)")
    async def removemembers(self, interaction: discord.Interaction, groupname: str, guild_id: str):
        if not await self._require_guild(interaction):
            return
        if not await self._require_approved(interaction):
            return

        groupname = groupname.strip()
        group_id = db.get_group_id(groupname)
        if not group_id:
            await interaction.response.send_message("Group not found.", ephemeral=True)
            return

        role = db.get_user_group_role(group_id, interaction.user.id)
        if role not in ("owner", "leader"):
            await interaction.response.send_message("You must be a **group leader** (or owner) to do that.", ephemeral=True)
            return

        try:
            gid = int(guild_id.strip())
        except Exception:
            await interaction.response.send_message("Invalid server ID.", ephemeral=True)
            return

        removed = db.leave_group(group_id, gid)
        if removed:
            await interaction.response.send_message("✅ Removed that server from the group.", ephemeral=True)
        else:
            await interaction.response.send_message("That server is not in the group.", ephemeral=True)

    # =========================
    # /groupmsg  (send to all servers in a group)
    # =========================
    @app_commands.command(name="groupmsg", description="Send a message to all servers in a group.")
    @app_commands.describe(groupname="Group name", message="Message to send")
    async def groupmsg(self, interaction: discord.Interaction, groupname: str, message: str):
        if not await self._require_guild(interaction):
            return
        if not await self._require_approved(interaction):
            return

        groupname = groupname.strip()
        group_id = db.get_group_id(groupname)
        if not group_id:
            await interaction.response.send_message("Group not found.", ephemeral=True)
            return

        # Require that THIS server is in the group
        if not db.guild_in_group(group_id, interaction.guild.id):
            await interaction.response.send_message("This server is not in that group.", ephemeral=True)
            return

        ok, wait_s = self._cooldown_ok(group_id, interaction.user.id)
        if not ok:
            await interaction.response.send_message(f"⏳ Cooldown: wait **{wait_s}s** before sending again.", ephemeral=True)
            return

        text = (message or "").strip()
        if not text:
            await interaction.response.send_message("Message cannot be empty.", ephemeral=True)
            return

        # hard limit to prevent mega spam
        if len(text) > 800:
            await interaction.response.send_message("Message too long (max 800 characters).", ephemeral=True)
            return

        text = _sanitize_broadcast_text(text)

        # Translate (user setting + group global toggle + global config)
        do_translate = TRANSLATE_ENABLED and bool(GOOGLE_TRANSLATE_API_KEY)
        user_translate_on = db.get_user_group_translate_enabled(interaction.user.id, default=True)
        group_translate_on = db.get_group_global_translate_enabled(group_id, default=True)
        do_translate = do_translate and user_translate_on and group_translate_on

        translated_text = text
        detected_lang = None
        did_translate = False
        err = None

        if do_translate and len(text) >= TRANSLATE_MIN_CHARS:
            translated_text, detected_lang, did_translate, err = await maybe_translate_to_english(
                GOOGLE_TRANSLATE_API_KEY,
                text,
                enabled=True,
                min_chars=TRANSLATE_MIN_CHARS
            )

        # Build embed
        embed = discord.Embed(title=f"📣 Group Message — {groupname}", description=translated_text)
        embed.set_author(name=str(interaction.user), icon_url=getattr(interaction.user.display_avatar, "url", None))
        embed.timestamp = discord.utils.utcnow()

        if did_translate and TRANSLATE_SHOW_ORIGINAL:
            embed.add_field(name="Original", value=text[:1024], inline=False)
        if detected_lang and did_translate:
            embed.set_footer(text=f"Translated from {detected_lang}")

        # Send to all servers in group
        targets = db.list_servers_in_group(group_id)
        sent = 0
        failed = 0

        for r in targets:
            guild_id = int(r["guild_id"])
            channel_id = _get_broadcast_channel_id_for_guild(guild_id)
            if not channel_id:
                failed += 1
                continue

            guild = self.bot.get_guild(guild_id)
            if not guild:
                failed += 1
                continue

            ch = guild.get_channel(channel_id)
            if not isinstance(ch, (discord.TextChannel, discord.Thread)):
                failed += 1
                continue

            try:
                await ch.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
                sent += 1
            except Exception:
                failed += 1

        await interaction.response.send_message(
            f"✅ Sent to **{sent}** server(s). Failed: **{failed}**." + (f"\nTranslation error: {err}" if err else ""),
            ephemeral=True
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(Groups(bot))
