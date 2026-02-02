# cogs/groups.py
from __future__ import annotations

import re
import time
from typing import Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from core import db  # uses your sqlite tables + approval system


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
    text = re.sub(r"<@!?\d+>", "[user]", text)     # user mention
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
    @app_commands.describe(groupname="Group name", servername="Server name to remove (must match stored name)")
    async def removemembers(self, interaction: discord.Interaction, groupname: str, servername: str):
        if not await self._require_guild(interaction):
            return
        if not await self._require_approved(interaction):
            return

        groupname = groupname.strip()
        servername = servername.strip()

        group_id = db.get_group_id(groupname)
        if not group_id:
            await interaction.response.send_message("Group not found.", ephemeral=True)
            return

        role = db.get_user_group_role(group_id, interaction.user.id)
        if role not in ("owner", "leader"):
            await interaction.response.send_message("You must be a **group leader** (or owner) to do that.", ephemeral=True)
            return

        removed = db.remove_server_from_group_by_name(group_id, servername)
        if removed:
            await interaction.response.send_message(f"✅ Removed **{servername}** from **{groupname}**.", ephemeral=True)
        else:
            await interaction.response.send_message(
                "No matching server name found in that group.\nTip: run `/listmembers` and copy the name exactly.",
                ephemeral=True
            )

    # =========================
    # Group broadcast core
    # =========================
    async def _group_broadcast(self, interaction: discord.Interaction, groupname: str, kind: str, message: str):
        if not await self._require_guild(interaction):
            return
        if not await self._require_approved(interaction):
            return

        groupname = groupname.strip()
        group_id = db.get_group_id(groupname)
        if not group_id:
            await interaction.response.send_message("Group not found.", ephemeral=True)
            return

        if not db.guild_in_group(group_id, interaction.guild.id):
            await interaction.response.send_message("This server is not in that group.", ephemeral=True)
            return

        ok, wait_s = self._cooldown_ok(group_id, interaction.user.id)
        if not ok:
            await interaction.response.send_message(f"Slow down — try again in **{wait_s}s**.", ephemeral=True)
            return

        clean = _sanitize_broadcast_text(message or "")
        if not clean.strip():
            clean = "(no details provided)"

        embed = discord.Embed(
            title=f"[GROUP {kind.upper()}] {groupname}",
            description=clean,
            timestamp=discord.utils.utcnow()
        )
        embed.set_footer(text=f"Sent by {interaction.user} • From {interaction.guild.name}")
        if interaction.user.display_avatar:
            embed.set_thumbnail(url=interaction.user.display_avatar.url)

        await interaction.response.send_message(f"📡 Sending **{kind}** to group **{groupname}**…", ephemeral=True)

        servers = db.list_servers_in_group(group_id)

        sent = 0
        failed = 0

        for s in servers:
            gid = int(s["guild_id"])
            channel_id = _get_broadcast_channel_id_for_guild(gid)
            if not channel_id:
                failed += 1
                continue

            channel = self.bot.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.bot.fetch_channel(channel_id)
                except Exception:
                    failed += 1
                    continue

            try:
                await channel.send(embed=embed)
                sent += 1
            except Exception:
                failed += 1

        try:
            await interaction.followup.send(f"✅ Done. Sent: **{sent}** | Failed: **{failed}**", ephemeral=True)
        except Exception:
            pass

    # =========================
    # /groupqrf /groupbattle /grouplogi
    # =========================
    @app_commands.command(name="groupqrf", description="Send a QRF broadcast to servers in a group.")
    @app_commands.describe(groupname="Group name", message="Details")
    async def groupqrf(self, interaction: discord.Interaction, groupname: str, message: str):
        await self._group_broadcast(interaction, groupname, "qrf", message)

    @app_commands.command(name="groupbattle", description="Send a battle broadcast to servers in a group.")
    @app_commands.describe(groupname="Group name", message="Details")
    async def groupbattle(self, interaction: discord.Interaction, groupname: str, message: str):
        await self._group_broadcast(interaction, groupname, "battle", message)

    @app_commands.command(name="grouplogi", description="Send a logistics broadcast to servers in a group.")
    @app_commands.describe(groupname="Group name", message="Details")
    async def grouplogi(self, interaction: discord.Interaction, groupname: str, message: str):
        await self._group_broadcast(interaction, groupname, "logi", message)


async def setup(bot: commands.Bot):
    await bot.add_cog(Groups(bot))

