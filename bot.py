import aiohttp
import base64
import io
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Optional

import discord
from discord.ext import commands
from discord import app_commands

TOKEN = os.getenv("DISCORD_TOKEN")

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is not set in Railway variables.")

intents = discord.Intents.default()
intents.guilds = True
intents.members = False
intents.message_content = False

bot = commands.Bot(command_prefix=commands.when_mentioned, intents=intents)

DB_PATH = "ticketbot.db"


# =========================
# DATABASE
# =========================
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS guild_config (
            guild_id INTEGER PRIMARY KEY,
            panel_channel_id INTEGER NOT NULL,
            panel_message_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            color_hex TEXT NOT NULL,
            banner_url TEXT,
            thumbnail_url TEXT,
            support_role_id INTEGER NOT NULL,
            log_channel_id INTEGER NOT NULL
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS ticket_options (
            guild_id INTEGER NOT NULL,
            option_index INTEGER NOT NULL,
            label TEXT NOT NULL,
            category_id INTEGER NOT NULL,
            PRIMARY KEY (guild_id, option_index)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            channel_id INTEGER PRIMARY KEY,
            guild_id INTEGER NOT NULL,
            opener_id INTEGER NOT NULL,
            option_label TEXT NOT NULL,
            status TEXT NOT NULL,
            claimed_by INTEGER,
            created_at TEXT NOT NULL,
            closed_at TEXT
        )
    """)

    conn.commit()
    conn.close()


def save_guild_config(
    guild_id: int,
    panel_channel_id: int,
    panel_message_id: int,
    title: str,
    description: str,
    color_hex: str,
    banner_url: Optional[str],
    thumbnail_url: Optional[str],
    support_role_id: int,
    log_channel_id: int,
):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO guild_config (
            guild_id, panel_channel_id, panel_message_id, title, description,
            color_hex, banner_url, thumbnail_url, support_role_id, log_channel_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(guild_id) DO UPDATE SET
            panel_channel_id=excluded.panel_channel_id,
            panel_message_id=excluded.panel_message_id,
            title=excluded.title,
            description=excluded.description,
            color_hex=excluded.color_hex,
            banner_url=excluded.banner_url,
            thumbnail_url=excluded.thumbnail_url,
            support_role_id=excluded.support_role_id,
            log_channel_id=excluded.log_channel_id
    """, (
        guild_id, panel_channel_id, panel_message_id, title, description,
        color_hex, banner_url, thumbnail_url, support_role_id, log_channel_id
    ))
    conn.commit()
    conn.close()


def get_guild_config(guild_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM guild_config WHERE guild_id = ?", (guild_id,))
    row = cur.fetchone()
    conn.close()
    return row


def clear_ticket_options(guild_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM ticket_options WHERE guild_id = ?", (guild_id,))
    conn.commit()
    conn.close()


def save_ticket_option(guild_id: int, option_index: int, label: str, category_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO ticket_options (guild_id, option_index, label, category_id)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(guild_id, option_index) DO UPDATE SET
            label=excluded.label,
            category_id=excluded.category_id
    """, (guild_id, option_index, label, category_id))
    conn.commit()
    conn.close()


def get_ticket_options(guild_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM ticket_options
        WHERE guild_id = ?
        ORDER BY option_index ASC
    """, (guild_id,))
    rows = cur.fetchall()
    conn.close()
    return rows


def create_ticket_record(channel_id: int, guild_id: int, opener_id: int, option_label: str):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tickets (
            channel_id, guild_id, opener_id, option_label, status,
            claimed_by, created_at, closed_at
        )
        VALUES (?, ?, ?, ?, 'open', NULL, ?, NULL)
    """, (
        channel_id, guild_id, opener_id, option_label,
        datetime.now(timezone.utc).isoformat()
    ))
    conn.commit()
    conn.close()


def get_ticket_by_channel(channel_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tickets WHERE channel_id = ?", (channel_id,))
    row = cur.fetchone()
    conn.close()
    return row


def get_open_ticket_for_user(guild_id: int, opener_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM tickets
        WHERE guild_id = ? AND opener_id = ? AND status = 'open'
        LIMIT 1
    """, (guild_id, opener_id))
    row = cur.fetchone()
    conn.close()
    return row


def set_ticket_claimed(channel_id: int, claimed_by: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        UPDATE tickets
        SET claimed_by = ?
        WHERE channel_id = ?
    """, (claimed_by, channel_id))
    conn.commit()
    conn.close()


def close_ticket_record(channel_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        UPDATE tickets
        SET status = 'closed', closed_at = ?
        WHERE channel_id = ?
    """, (datetime.now(timezone.utc).isoformat(), channel_id))
    conn.commit()
    conn.close()


def delete_ticket_record(channel_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM tickets WHERE channel_id = ?", (channel_id,))
    conn.commit()
    conn.close()


# =========================
# SETUP SESSION
# =========================
class SetupSession:
    def __init__(
        self,
        guild_id: int,
        user_id: int,
        panel_channel_id: int,
        log_channel_id: int,
        support_role_id: int,
        option_1_category_id: int,
        option_2_category_id: Optional[int],
        option_3_category_id: Optional[int],
        option_3_name: Optional[str],
        banner_url: str,
        thumbnail_url: str,
    ):
        self.guild_id = guild_id
        self.user_id = user_id

        self.panel_channel_id = panel_channel_id
        self.log_channel_id = log_channel_id
        self.support_role_id = support_role_id

        self.option_1_category_id = option_1_category_id
        self.option_2_category_id = option_2_category_id
        self.option_3_category_id = option_3_category_id
        self.option_3_name = option_3_name.strip() if option_3_name else None

        self.banner_url = banner_url
        self.thumbnail_url = thumbnail_url

        self.title: Optional[str] = None
        self.description: Optional[str] = None
        self.color_hex: Optional[str] = None
        self.option_1_name: Optional[str] = None
        self.option_2_name: Optional[str] = None


setup_sessions: dict[tuple[int, int], SetupSession] = {}


def get_setup_session(guild_id: int, user_id: int) -> Optional[SetupSession]:
    return setup_sessions.get((guild_id, user_id))


def clear_setup_session(guild_id: int, user_id: int):
    setup_sessions.pop((guild_id, user_id), None)


# =========================
# HELPERS
# =========================
def normalize_hex(value: str) -> str:
    clean = value.strip().replace("#", "")
    if not re.fullmatch(r"[0-9a-fA-F]{6}", clean):
        raise ValueError("Invalid hex color.")
    return f"#{clean.upper()}"


def hex_to_color(value: str) -> discord.Color:
    clean = value.replace("#", "")
    return discord.Color(int(clean, 16))


def clean_channel_name(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9\- ]", "", text)
    text = re.sub(r"\s+", "-", text).strip("-")
    text = re.sub(r"-{2,}", "-", text)
    return text[:80] if text else "ticket"


def is_image_attachment(att: discord.Attachment) -> bool:
    if att.content_type and att.content_type.startswith("image/"):
        return True
    filename = att.filename.lower()
    return filename.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp"))


def base_embed(
    guild_id: Optional[int],
    title: str,
    description: str,
    color_override: Optional[discord.Color] = None
) -> discord.Embed:
    if color_override is not None:
        color = color_override
    else:
        config = get_guild_config(guild_id) if guild_id else None
        color = hex_to_color(config["color_hex"]) if config else discord.Color.green()

    embed = discord.Embed(
        title=title,
        description=description,
        color=color
    )
    embed.set_footer(text="made by @fntsheetz")
    return embed


def build_panel_embed(guild_id: int) -> discord.Embed:
    config = get_guild_config(guild_id)
    if not config:
        return base_embed(
            guild_id=None,
            title="Ticket panel not configured",
            description="This server has not configured the ticket panel yet.",
            color_override=discord.Color.red()
        )

    embed = base_embed(
        guild_id=guild_id,
        title=config["title"],
        description=config["description"]
    )

    if config["thumbnail_url"]:
        embed.set_thumbnail(url=config["thumbnail_url"])

    if config["banner_url"]:
        embed.set_image(url=config["banner_url"])

    return embed


def build_ticket_embed(guild_id: int, option_label: str, opener: discord.Member) -> discord.Embed:
    return base_embed(
        guild_id=guild_id,
        title=option_label,
        description=(
            f"{opener.mention}, your ticket has been created.\n\n"
            f"Please explain everything clearly.\n"
            f"A member of the support team will reply here."
        )
    )


def is_support_or_admin(member: discord.Member, guild_id: int) -> bool:
    if member.guild_permissions.administrator:
        return True

    config = get_guild_config(guild_id)
    if not config:
        return False

    role = member.guild.get_role(config["support_role_id"])
    return role in member.roles if role else False


async def build_transcript_text(channel: discord.TextChannel) -> str:
    lines = []
    lines.append(f"Transcript for #{channel.name}")
    lines.append(f"Channel ID: {channel.id}")
    lines.append(f"Guild: {channel.guild.name} ({channel.guild.id})")
    lines.append("-" * 80)

    messages = [msg async for msg in channel.history(limit=None, oldest_first=True)]

    for msg in messages:
        created = msg.created_at.strftime("%Y-%m-%d %H:%M:%S UTC")
        author = f"{msg.author} ({msg.author.id})"
        content = msg.content if msg.content else ""

        attachment_text = ""
        if msg.attachments:
            urls = ", ".join(att.url for att in msg.attachments)
            attachment_text = f" [Attachments: {urls}]"

        embed_text = ""
        if msg.embeds:
            embed_parts = []
            for e in msg.embeds:
                parts = []
                if e.title:
                    parts.append(f"title={e.title}")
                if e.description:
                    parts.append(f"description={e.description}")
                if parts:
                    embed_parts.append(" | ".join(parts))
            if embed_parts:
                embed_text = f" [Embeds: {' || '.join(embed_parts)}]"

        lines.append(f"[{created}] {author}: {content}{attachment_text}{embed_text}")

    return "\n".join(lines)


async def send_log(
    guild: discord.Guild,
    title: str,
    description: str,
    file: Optional[discord.File] = None
):
    config = get_guild_config(guild.id)
    if not config:
        return

    log_channel = guild.get_channel(config["log_channel_id"])
    if not isinstance(log_channel, discord.TextChannel):
        return

    embed = base_embed(guild.id, title, description)

    try:
        await log_channel.send(embed=embed, file=file)
    except discord.Forbidden:
        pass


async def set_guild_profile(
    guild_id: int,
    nickname: Optional[str] = None,
    avatar_attachment: Optional[discord.Attachment] = None
) -> tuple[bool, str]:
    payload = {}

    if nickname is not None:
        payload["nick"] = nickname

    if avatar_attachment is not None:
        if not is_image_attachment(avatar_attachment):
            return False, "Avatar attachment must be an image."

        raw = await avatar_attachment.read()
        mime = avatar_attachment.content_type or "image/png"
        b64 = base64.b64encode(raw).decode("utf-8")
        payload["avatar"] = f"data:{mime};base64,{b64}"

    if not payload:
        return False, "Nothing to update."

    headers = {
        "Authorization": f"Bot {TOKEN}",
        "Content-Type": "application/json"
    }

    url = f"https://discord.com/api/v10/guilds/{guild_id}/members/@me"

    async with aiohttp.ClientSession() as session:
        async with session.patch(url, json=payload, headers=headers) as resp:
            if 200 <= resp.status < 300:
                return True, "Guild profile updated successfully."

            text = await resp.text()
            return False, f"Discord API error {resp.status}: {text}"


# =========================
# SETUP MODAL
# =========================
class SetupTextModal(discord.ui.Modal, title="Ticket Panel Setup"):
    panel_title = discord.ui.TextInput(
        label="Ticket panel title",
        placeholder="Vision Tickets",
        max_length=100
    )

    description = discord.ui.TextInput(
        label="Description",
        placeholder="Write the panel description here",
        style=discord.TextStyle.paragraph,
        max_length=1000
    )

    color_hex = discord.ui.TextInput(
        label="Embed color hexcode",
        placeholder="#00FF00",
        max_length=7
    )

    option_1_name = discord.ui.TextInput(
        label="Ticket option 1",
        placeholder="Support Ticket",
        max_length=100
    )

    option_2_name = discord.ui.TextInput(
        label="Ticket option 2 (optional)",
        placeholder="Purchase Ticket",
        required=False,
        max_length=100
    )

    def __init__(self, session: SetupSession):
        super().__init__()
        self.session = session

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            return

        try:
            self.session.color_hex = normalize_hex(str(self.color_hex))
        except ValueError:
            await interaction.response.send_message(
                embed=base_embed(
                    None,
                    "Setup Failed",
                    "Invalid hex color. Example: `#00FF00`.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        self.session.title = str(self.panel_title).strip()
        self.session.description = str(self.description).strip()
        self.session.option_1_name = str(self.option_1_name).strip()
        self.session.option_2_name = str(self.option_2_name).strip() or None

        if self.session.option_2_category_id and not self.session.option_2_name:
            await interaction.response.send_message(
                embed=base_embed(
                    None,
                    "Setup Failed",
                    "You selected a category for option 2 but left the option name empty.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        if self.session.option_2_name and not self.session.option_2_category_id:
            await interaction.response.send_message(
                embed=base_embed(
                    None,
                    "Setup Failed",
                    "You entered option 2 name but did not choose a category for option 2.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        if self.session.option_3_name and not self.session.option_3_category_id:
            await interaction.response.send_message(
                embed=base_embed(
                    None,
                    "Setup Failed",
                    "You entered option 3 name but did not choose a category for option 3.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        panel_channel = interaction.guild.get_channel(self.session.panel_channel_id)
        if not isinstance(panel_channel, discord.TextChannel):
            await interaction.response.send_message(
                embed=base_embed(
                    None,
                    "Setup Failed",
                    "Panel channel is invalid.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        clear_ticket_options(interaction.guild.id)
        save_ticket_option(interaction.guild.id, 1, self.session.option_1_name, self.session.option_1_category_id)

        if self.session.option_2_name and self.session.option_2_category_id:
            save_ticket_option(interaction.guild.id, 2, self.session.option_2_name, self.session.option_2_category_id)

        if self.session.option_3_name and self.session.option_3_category_id:
            save_ticket_option(interaction.guild.id, 3, self.session.option_3_name, self.session.option_3_category_id)

        embed = discord.Embed(
            title=self.session.title,
            description=self.session.description,
            color=hex_to_color(self.session.color_hex)
        )
        embed.set_thumbnail(url=self.session.thumbnail_url)
        embed.set_image(url=self.session.banner_url)
        embed.set_footer(text="made by @fntsheetz")

        panel_message = await panel_channel.send(
            embed=embed,
            view=TicketPanelView(interaction.guild.id)
        )

        save_guild_config(
            guild_id=interaction.guild.id,
            panel_channel_id=self.session.panel_channel_id,
            panel_message_id=panel_message.id,
            title=self.session.title,
            description=self.session.description,
            color_hex=self.session.color_hex,
            banner_url=self.session.banner_url,
            thumbnail_url=self.session.thumbnail_url,
            support_role_id=self.session.support_role_id,
            log_channel_id=self.session.log_channel_id
        )

        bot.add_view(TicketPanelView(interaction.guild.id), message_id=panel_message.id)
        clear_setup_session(interaction.guild.id, interaction.user.id)

        await interaction.response.send_message(
            embed=base_embed(
                interaction.guild.id,
                "Setup Complete",
                f"Ticket panel created in {panel_channel.mention}."
            ),
            ephemeral=True
        )


# =========================
# PANEL VIEW
# =========================
class TicketDropdown(discord.ui.Select):
    def __init__(self, guild_id: int):
        rows = get_ticket_options(guild_id)
        options = [
            discord.SelectOption(label=row["label"][:100], value=str(row["option_index"]))
            for row in rows
        ]

        super().__init__(
            placeholder="Make a selection",
            min_values=1,
            max_values=1,
            options=options,
            custom_id=f"ticket_dropdown:{guild_id}"
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        guild = interaction.guild
        opener = interaction.user
        config = get_guild_config(guild.id)

        if not config:
            await interaction.response.send_message(
                embed=base_embed(
                    guild.id,
                    "Error",
                    "Ticket system is not configured.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        existing = get_open_ticket_for_user(guild.id, opener.id)
        if existing:
            existing_channel = guild.get_channel(existing["channel_id"])
            if existing_channel:
                await interaction.response.send_message(
                    embed=base_embed(
                        guild.id,
                        "Open Ticket Found",
                        f"You already have an open ticket: {existing_channel.mention}"
                    ),
                    ephemeral=True
                )
                return

        selected_index = int(self.values[0])
        rows = get_ticket_options(guild.id)
        selected = next((r for r in rows if r["option_index"] == selected_index), None)

        if not selected:
            await interaction.response.send_message(
                embed=base_embed(
                    guild.id,
                    "Error",
                    "That ticket option is no longer configured.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        category = guild.get_channel(selected["category_id"])
        if not isinstance(category, discord.CategoryChannel):
            await interaction.response.send_message(
                embed=base_embed(
                    guild.id,
                    "Error",
                    "The configured category is invalid.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        support_role = guild.get_role(config["support_role_id"])
        if not support_role:
            await interaction.response.send_message(
                embed=base_embed(
                    guild.id,
                    "Error",
                    "The support role is invalid.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        channel_name = clean_channel_name(f"{selected['label']}-{opener.name}")

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            opener: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                attach_files=True,
                embed_links=True
            ),
            support_role: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                attach_files=True,
                embed_links=True,
                manage_messages=True
            ),
            guild.me: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                attach_files=True,
                embed_links=True,
                manage_channels=True,
                manage_messages=True
            )
        }

        try:
            ticket_channel = await guild.create_text_channel(
                name=channel_name,
                category=category,
                overwrites=overwrites,
                reason=f"Ticket created by {opener} ({opener.id})"
            )
        except discord.Forbidden:
            await interaction.response.send_message(
                embed=base_embed(
                    guild.id,
                    "Error",
                    "I do not have permission to create channels in that category.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        create_ticket_record(ticket_channel.id, guild.id, opener.id, selected["label"])

        await ticket_channel.send(
            content=f"{support_role.mention} {opener.mention}",
            embed=build_ticket_embed(guild.id, selected["label"], opener),
            view=OpenTicketView()
        )

        await send_log(
            guild,
            "Ticket Opened",
            (
                f"User: {opener.mention}\n"
                f"Channel: {ticket_channel.mention}\n"
                f"Type: {selected['label']}"
            )
        )

        await interaction.response.send_message(
            embed=base_embed(
                guild.id,
                "Ticket Created",
                f"Your ticket has been created: {ticket_channel.mention}"
            ),
            ephemeral=True
        )


class TicketPanelView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.add_item(TicketDropdown(guild_id))


# =========================
# TICKET CONTROL VIEWS
# =========================
class ClaimTicketButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="Claim Ticket",
            style=discord.ButtonStyle.secondary,
            custom_id="ticket_claim_button"
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        if not isinstance(interaction.channel, discord.TextChannel):
            return

        if not is_support_or_admin(interaction.user, interaction.guild.id):
            await interaction.response.send_message(
                embed=base_embed(
                    interaction.guild.id,
                    "Access Denied",
                    "Only the support team or admins can claim tickets.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        ticket = get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            await interaction.response.send_message(
                embed=base_embed(
                    interaction.guild.id,
                    "Error",
                    "This is not a tracked ticket channel.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        if ticket["status"] != "open":
            await interaction.response.send_message(
                embed=base_embed(
                    interaction.guild.id,
                    "Error",
                    "This ticket is already closed.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        if ticket["claimed_by"] == interaction.user.id:
            await interaction.response.send_message(
                embed=base_embed(
                    interaction.guild.id,
                    "Already Claimed",
                    "You already claimed this ticket."
                ),
                ephemeral=True
            )
            return

        set_ticket_claimed(interaction.channel.id, interaction.user.id)

        await interaction.response.send_message(
            embed=base_embed(
                interaction.guild.id,
                "Ticket Claimed",
                f"Ticket claimed by {interaction.user.mention}."
            )
        )

        await send_log(
            interaction.guild,
            "Ticket Claimed",
            (
                f"Channel: {interaction.channel.mention}\n"
                f"Claimed by: {interaction.user.mention}"
            )
        )


class CloseTicketButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="Close Ticket",
            style=discord.ButtonStyle.danger,
            custom_id="ticket_close_button"
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        if not isinstance(interaction.channel, discord.TextChannel):
            return

        if not is_support_or_admin(interaction.user, interaction.guild.id):
            await interaction.response.send_message(
                embed=base_embed(
                    interaction.guild.id,
                    "Access Denied",
                    "Only the support team or admins can close tickets.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        ticket = get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            await interaction.response.send_message(
                embed=base_embed(
                    interaction.guild.id,
                    "Error",
                    "This is not a tracked ticket channel.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        if ticket["status"] == "closed":
            await interaction.response.send_message(
                embed=base_embed(
                    interaction.guild.id,
                    "Already Closed",
                    "This ticket is already closed."
                ),
                ephemeral=True
            )
            return

        config = get_guild_config(interaction.guild.id)
        support_role = interaction.guild.get_role(config["support_role_id"]) if config else None

        try:
            await interaction.channel.set_permissions(
                discord.Object(id=ticket["opener_id"]),
                view_channel=False
            )

            if support_role:
                await interaction.channel.set_permissions(
                    support_role,
                    view_channel=True,
                    send_messages=True,
                    read_message_history=True,
                    attach_files=True,
                    embed_links=True,
                    manage_messages=True
                )
        except discord.Forbidden:
            await interaction.response.send_message(
                embed=base_embed(
                    interaction.guild.id,
                    "Error",
                    "I do not have permission to update channel permissions.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        close_ticket_record(interaction.channel.id)

        closed_embed = base_embed(
            interaction.guild.id,
            "Ticket Closed",
            f"Ticket closed by {interaction.user.mention}."
        )

        await interaction.response.edit_message(
            embed=closed_embed,
            view=ClosedTicketView()
        )

        await send_log(
            interaction.guild,
            "Ticket Closed",
            (
                f"Channel: #{interaction.channel.name}\n"
                f"Closed by: {interaction.user.mention}"
            )
        )


class DeleteTicketButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="Delete Ticket",
            style=discord.ButtonStyle.danger,
            custom_id="ticket_delete_button"
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        if not isinstance(interaction.channel, discord.TextChannel):
            return

        if not is_support_or_admin(interaction.user, interaction.guild.id):
            await interaction.response.send_message(
                embed=base_embed(
                    interaction.guild.id,
                    "Access Denied",
                    "Only the support team or admins can delete tickets.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        ticket = get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            await interaction.response.send_message(
                embed=base_embed(
                    interaction.guild.id,
                    "Error",
                    "This is not a tracked ticket channel.",
                    color_override=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        transcript_text = await build_transcript_text(interaction.channel)
        transcript_bytes = transcript_text.encode("utf-8", errors="ignore")
        transcript_file = discord.File(
            io.BytesIO(transcript_bytes),
            filename=f"transcript-{interaction.channel.name}.txt"
        )

        opener_text = f"<@{ticket['opener_id']}>"

        await send_log(
            interaction.guild,
            "Ticket Deleted",
            (
                f"Channel: #{interaction.channel.name}\n"
                f"Opened by: {opener_text}\n"
                f"Type: {ticket['option_label']}\n"
                f"Deleted by: {interaction.user.mention}"
            ),
            file=transcript_file
        )

        delete_ticket_record(interaction.channel.id)

        await interaction.response.send_message(
            embed=base_embed(
                interaction.guild.id,
                "Deleting Ticket",
                "The ticket is being deleted."
            )
        )

        try:
            await interaction.channel.delete(reason=f"Ticket deleted by {interaction.user}")
        except discord.Forbidden:
            pass


class OpenTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(ClaimTicketButton())
        self.add_item(CloseTicketButton())


class ClosedTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(DeleteTicketButton())


# =========================
# COMMANDS
# =========================
@bot.tree.command(name="setup", description="Open the ticket setup form")
@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
async def setup(
    interaction: discord.Interaction,
    panel_channel: discord.TextChannel,
    log_channel: discord.TextChannel,
    support_team: discord.Role,
    option_1_category: discord.CategoryChannel,
    banner: discord.Attachment,
    small_picture: discord.Attachment,
    option_2_category: Optional[discord.CategoryChannel] = None,
    option_3_category: Optional[discord.CategoryChannel] = None,
    option_3_name: Optional[str] = None,
):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message(
            embed=base_embed(
                None,
                "Error",
                "This command can only be used in a server.",
                color_override=discord.Color.red()
            ),
            ephemeral=True
        )
        return

    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            embed=base_embed(
                None,
                "Access Denied",
                "Only users with Administrator can use this command.",
                color_override=discord.Color.red()
            ),
            ephemeral=True
        )
        return

    if not is_image_attachment(banner):
        await interaction.response.send_message(
            embed=base_embed(
                None,
                "Setup Failed",
                "The banner attachment must be an image.",
                color_override=discord.Color.red()
            ),
            ephemeral=True
        )
        return

    if not is_image_attachment(small_picture):
        await interaction.response.send_message(
            embed=base_embed(
                None,
                "Setup Failed",
                "The small picture attachment must be an image.",
                color_override=discord.Color.red()
            ),
            ephemeral=True
        )
        return

    if option_3_name and not option_3_category:
        await interaction.response.send_message(
            embed=base_embed(
                None,
                "Setup Failed",
                "If you enter option 3 name, you must also choose option 3 category.",
                color_override=discord.Color.red()
            ),
            ephemeral=True
        )
        return

    if option_3_category and not option_3_name:
        await interaction.response.send_message(
            embed=base_embed(
                None,
                "Setup Failed",
                "If you choose option 3 category, you must also enter option 3 name.",
                color_override=discord.Color.red()
            ),
            ephemeral=True
        )
        return

    session = SetupSession(
        guild_id=interaction.guild.id,
        user_id=interaction.user.id,
        panel_channel_id=panel_channel.id,
        log_channel_id=log_channel.id,
        support_role_id=support_team.id,
        option_1_category_id=option_1_category.id,
        option_2_category_id=option_2_category.id if option_2_category else None,
        option_3_category_id=option_3_category.id if option_3_category else None,
        option_3_name=option_3_name,
        banner_url=banner.url,
        thumbnail_url=small_picture.url
    )
    setup_sessions[(interaction.guild.id, interaction.user.id)] = session

    await interaction.response.send_modal(SetupTextModal(session))


@bot.tree.command(name="remind", description="DM a user to reply in their ticket")
@app_commands.guild_only()
async def remind(
    interaction: discord.Interaction,
    user: discord.Member,
    message: str
):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return

    if not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message(
            embed=base_embed(
                interaction.guild.id,
                "Error",
                "This command must be used inside a ticket channel.",
                color_override=discord.Color.red()
            ),
            ephemeral=True
        )
        return

    if not is_support_or_admin(interaction.user, interaction.guild.id):
        await interaction.response.send_message(
            embed=base_embed(
                interaction.guild.id,
                "Access Denied",
                "Only the support team or admins can use this command.",
                color_override=discord.Color.red()
            ),
            ephemeral=True
        )
        return

    ticket = get_ticket_by_channel(interaction.channel.id)
    if not ticket:
        await interaction.response.send_message(
            embed=base_embed(
                interaction.guild.id,
                "Error",
                "This command can only be used inside a ticket channel.",
                color_override=discord.Color.red()
            ),
            ephemeral=True
        )
        return

    if ticket["opener_id"] != user.id:
        await interaction.response.send_message(
            embed=base_embed(
                interaction.guild.id,
                "Error",
                "That user is not the opener of this ticket.",
                color_override=discord.Color.red()
            ),
            ephemeral=True
        )
        return

    config = get_guild_config(interaction.guild.id)
    dm_embed = discord.Embed(
        title="Ticket Reminder",
        description=(
            f"You have an open ticket in **{interaction.guild.name}**.\n\n"
            f"Message from support:\n{message}\n\n"
            f"Ticket channel: #{interaction.channel.name}"
        ),
        color=hex_to_color(config["color_hex"])
    )
    dm_embed.set_footer(text="made by @fntsheetz")

    try:
        await user.send(embed=dm_embed)
    except discord.Forbidden:
        await interaction.response.send_message(
            embed=base_embed(
                interaction.guild.id,
                "DM Failed",
                "I could not DM that user.",
                color_override=discord.Color.red()
            ),
            ephemeral=True
        )
        return

    await interaction.response.send_message(
        embed=base_embed(
            interaction.guild.id,
            "Reminder Sent",
            f"Reminder sent to {user.mention}."
        ),
        ephemeral=True
    )

    await send_log(
        interaction.guild,
        "Ticket Reminder Sent",
        (
            f"Channel: {interaction.channel.mention}\n"
            f"To: {user.mention}\n"
            f"By: {interaction.user.mention}\n"
            f"Message: {message}"
        )
    )


@bot.tree.command(name="serverprofile", description="Change the bot nickname and avatar for this server")
@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
async def serverprofile(
    interaction: discord.Interaction,
    nickname: Optional[str] = None,
    avatar: Optional[discord.Attachment] = None,
):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return

    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            embed=base_embed(
                None,
                "Access Denied",
                "Only users with Administrator can use this command.",
                color_override=discord.Color.red()
            ),
            ephemeral=True
        )
        return

    if avatar is not None and not is_image_attachment(avatar):
        await interaction.response.send_message(
            embed=base_embed(
                None,
                "Update Failed",
                "Avatar attachment must be an image.",
                color_override=discord.Color.red()
            ),
            ephemeral=True
        )
        return

    success, message = await set_guild_profile(
        guild_id=interaction.guild.id,
        nickname=nickname,
        avatar_attachment=avatar
    )

    if success:
        await interaction.response.send_message(
            embed=base_embed(
                interaction.guild.id,
                "Server Profile Updated",
                "The bot profile was updated for this server."
            ),
            ephemeral=True
        )
    else:
        await interaction.response.send_message(
            embed=base_embed(
                interaction.guild.id,
                "Update Failed",
                message,
                color_override=discord.Color.red()
            ),
            ephemeral=True
        )


@setup.error
async def setup_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    msg = f"Setup failed: {error}"
    embed = base_embed(
        interaction.guild.id if interaction.guild else None,
        "Setup Failed",
        msg,
        color_override=discord.Color.red()
    )
    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


@remind.error
async def remind_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    msg = f"Remind failed: {error}"
    embed = base_embed(
        interaction.guild.id if interaction.guild else None,
        "Remind Failed",
        msg,
        color_override=discord.Color.red()
    )
    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


@serverprofile.error
async def serverprofile_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    msg = f"Server profile update failed: {error}"
    embed = base_embed(
        interaction.guild.id if interaction.guild else None,
        "Update Failed",
        msg,
        color_override=discord.Color.red()
    )
    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


# =========================
# READY
# =========================
@bot.event
async def on_ready():
    init_db()

    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} commands")
    except Exception as e:
        print(f"Command sync failed: {e}")

    bot.add_view(OpenTicketView())
    bot.add_view(ClosedTicketView())

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT guild_id, panel_message_id FROM guild_config")
    rows = cur.fetchall()
    conn.close()

    for row in rows:
        try:
            bot.add_view(TicketPanelView(row["guild_id"]), message_id=row["panel_message_id"])
        except Exception as e:
            print(f"Failed to restore panel view for guild {row['guild_id']}: {e}")

    print(f"Logged in as {bot.user} ({bot.user.id})")


bot.run(TOKEN)
