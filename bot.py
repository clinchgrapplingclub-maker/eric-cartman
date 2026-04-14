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
    def __init__(self, guild_id: int, user_id: int, banner_url: str, thumbnail_url: str):
        self.guild_id = guild_id
        self.user_id = user_id

        self.title: Optional[str] = None
        self.description: Optional[str] = None
        self.color_hex: Optional[str] = None

        self.banner_url: str = banner_url
        self.thumbnail_url: str = thumbnail_url

        self.option_1_name: Optional[str] = None
        self.option_1_category_id: Optional[int] = None

        self.option_2_name: Optional[str] = None
        self.option_2_category_id: Optional[int] = None

        self.option_3_name: Optional[str] = None
        self.option_3_category_id: Optional[int] = None

        self.panel_channel_id: Optional[int] = None
        self.log_channel_id: Optional[int] = None
        self.support_role_id: Optional[int] = None


setup_sessions: dict[tuple[int, int], SetupSession] = {}


def get_setup_session(guild_id: int, user_id: int) -> Optional[SetupSession]:
    return setup_sessions.get((guild_id, user_id))


def clear_setup_session(guild_id: int, user_id: int):
    setup_sessions.pop((guild_id, user_id), None)


def build_setup_summary_embed(guild: discord.Guild, session: SetupSession) -> discord.Embed:
    color = discord.Color.green()
    if session.color_hex:
        try:
            color = hex_to_color(session.color_hex)
        except Exception:
            pass

    panel_channel = guild.get_channel(session.panel_channel_id) if session.panel_channel_id else None
    log_channel = guild.get_channel(session.log_channel_id) if session.log_channel_id else None
    support_role = guild.get_role(session.support_role_id) if session.support_role_id else None

    option_1_cat = guild.get_channel(session.option_1_category_id) if session.option_1_category_id else None
    option_2_cat = guild.get_channel(session.option_2_category_id) if session.option_2_category_id else None
    option_3_cat = guild.get_channel(session.option_3_category_id) if session.option_3_category_id else None

    def fmt(value: Optional[str]) -> str:
        return value if value else "`Not set`"

    embed = discord.Embed(
        title="Ticket Setup",
        description="Use the buttons below to finish the setup.",
        color=color
    )

    embed.add_field(
        name="Panel",
        value=(
            f"**Title:** {fmt(session.title)}\n"
            f"**Description:** {fmt(session.description)}\n"
            f"**Color:** {fmt(session.color_hex)}"
        ),
        inline=False
    )

    embed.add_field(
        name="Channels / Role",
        value=(
            f"**Panel Channel:** {panel_channel.mention if panel_channel else '`Not set`'}\n"
            f"**Log Channel:** {log_channel.mention if log_channel else '`Not set`'}\n"
            f"**Support Team:** {support_role.mention if support_role else '`Not set`'}"
        ),
        inline=False
    )

    embed.add_field(
        name="Ticket Options",
        value=(
            f"**1.** {fmt(session.option_1_name)} - {option_1_cat.mention if option_1_cat else '`Category not set`'}\n"
            f"**2.** {fmt(session.option_2_name)} - {option_2_cat.mention if option_2_cat else ('`Not needed`' if not session.option_2_name else '`Category not set`')}\n"
            f"**3.** {fmt(session.option_3_name)} - {option_3_cat.mention if option_3_cat else ('`Not needed`' if not session.option_3_name else '`Category not set`')}"
        ),
        inline=False
    )

    embed.add_field(
        name="Images",
        value="**Banner:** uploaded\n**Small Picture:** uploaded",
        inline=False
    )

    embed.set_thumbnail(url=session.thumbnail_url)
    embed.set_image(url=session.banner_url)
    embed.set_footer(text="made by @fntsheetz")
    return embed


# =========================
# HELPERS
# =========================
def build_panel_embed(guild_id: int) -> discord.Embed:
    config = get_guild_config(guild_id)
    if not config:
        return discord.Embed(
            title="Ticket panel not configured",
            description="This server has not configured the ticket panel yet.",
            color=discord.Color.red()
        )

    embed = discord.Embed(
        title=config["title"],
        description=config["description"],
        color=hex_to_color(config["color_hex"])
    )

    if config["thumbnail_url"]:
        embed.set_thumbnail(url=config["thumbnail_url"])

    if config["banner_url"]:
        embed.set_image(url=config["banner_url"])

    embed.set_footer(text="made by @fntsheetz")
    return embed


def build_ticket_embed(guild_id: int, option_label: str, opener: discord.Member) -> discord.Embed:
    config = get_guild_config(guild_id)
    color = hex_to_color(config["color_hex"]) if config else discord.Color.green()

    embed = discord.Embed(
        title=option_label,
        description=(
            f"{opener.mention}, your ticket has been created.\n\n"
            f"Please explain everything clearly.\n"
            f"A member of the support team will reply here."
        ),
        color=color
    )
    embed.set_footer(text="made by @fntsheetz")
    return embed


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

    embed = discord.Embed(
        title=title,
        description=description,
        color=hex_to_color(config["color_hex"])
    )
    embed.set_footer(text="made by @fntsheetz")

    try:
        await log_channel.send(embed=embed, file=file)
    except discord.Forbidden:
        pass


# =========================
# SETUP MODALS
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
        try:
            self.session.color_hex = normalize_hex(str(self.color_hex))
        except ValueError:
            await interaction.response.send_message(
                "Invalid hex color. Example: #00FF00",
                ephemeral=True
            )
            return

        self.session.title = str(self.panel_title).strip()
        self.session.description = str(self.description).strip()
        self.session.option_1_name = str(self.option_1_name).strip()
        self.session.option_2_name = str(self.option_2_name).strip() or None

        await interaction.response.send_message(
            embed=build_setup_summary_embed(interaction.guild, self.session),
            view=SetupHubView(self.session),
            ephemeral=True
        )


class Option3NameModal(discord.ui.Modal, title="Ticket Option 3"):
    option_3_name = discord.ui.TextInput(
        label="Ticket option 3 (leave blank to remove)",
        placeholder="Report Ticket",
        required=False,
        max_length=100
    )

    def __init__(self, session: SetupSession):
        super().__init__()
        self.session = session

    async def on_submit(self, interaction: discord.Interaction):
        value = str(self.option_3_name).strip() or None
        self.session.option_3_name = value

        if not value:
            self.session.option_3_category_id = None

        await interaction.response.send_message(
            embed=build_setup_summary_embed(interaction.guild, self.session),
            view=SetupHubView(self.session),
            ephemeral=True
        )


# =========================
# SETUP SELECTS
# =========================
class PanelChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, session: SetupSession):
        super().__init__(
            placeholder="Choose panel channel",
            min_values=1,
            max_values=1,
            channel_types=[discord.ChannelType.text]
        )
        self.session = session

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("This setup is not yours.", ephemeral=True)
            return

        channel = self.values[0]
        self.session.panel_channel_id = channel.id

        await interaction.response.send_message(
            f"Panel channel set to {channel.mention}.",
            ephemeral=True
        )


class LogChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, session: SetupSession):
        super().__init__(
            placeholder="Choose log channel",
            min_values=1,
            max_values=1,
            channel_types=[discord.ChannelType.text]
        )
        self.session = session

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("This setup is not yours.", ephemeral=True)
            return

        channel = self.values[0]
        self.session.log_channel_id = channel.id

        await interaction.response.send_message(
            f"Log channel set to {channel.mention}.",
            ephemeral=True
        )


class SupportRoleSelect(discord.ui.RoleSelect):
    def __init__(self, session: SetupSession):
        super().__init__(
            placeholder="Choose support team role",
            min_values=1,
            max_values=1
        )
        self.session = session

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("This setup is not yours.", ephemeral=True)
            return

        role = self.values[0]
        self.session.support_role_id = role.id

        await interaction.response.send_message(
            f"Support role set to {role.mention}.",
            ephemeral=True
        )


class Option1CategorySelect(discord.ui.ChannelSelect):
    def __init__(self, session: SetupSession):
        super().__init__(
            placeholder="Choose category for option 1",
            min_values=1,
            max_values=1,
            channel_types=[discord.ChannelType.category]
        )
        self.session = session

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("This setup is not yours.", ephemeral=True)
            return

        category = self.values[0]
        self.session.option_1_category_id = category.id

        await interaction.response.send_message(
            f"Option 1 category set to **{category.name}**.",
            ephemeral=True
        )


class Option2CategorySelect(discord.ui.ChannelSelect):
    def __init__(self, session: SetupSession):
        super().__init__(
            placeholder="Choose category for option 2",
            min_values=1,
            max_values=1,
            channel_types=[discord.ChannelType.category]
        )
        self.session = session

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("This setup is not yours.", ephemeral=True)
            return

        category = self.values[0]
        self.session.option_2_category_id = category.id

        await interaction.response.send_message(
            f"Option 2 category set to **{category.name}**.",
            ephemeral=True
        )


class Option3CategorySelect(discord.ui.ChannelSelect):
    def __init__(self, session: SetupSession):
        super().__init__(
            placeholder="Choose category for option 3",
            min_values=1,
            max_values=1,
            channel_types=[discord.ChannelType.category]
        )
        self.session = session

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("This setup is not yours.", ephemeral=True)
            return

        category = self.values[0]
        self.session.option_3_category_id = category.id

        await interaction.response.send_message(
            f"Option 3 category set to **{category.name}**.",
            ephemeral=True
        )


# =========================
# SETUP VIEWS
# =========================
class ChannelsRoleView(discord.ui.View):
    def __init__(self, session: SetupSession):
        super().__init__(timeout=900)
        self.session = session
        self.add_item(PanelChannelSelect(session))
        self.add_item(LogChannelSelect(session))
        self.add_item(SupportRoleSelect(session))


class CategoriesView(discord.ui.View):
    def __init__(self, session: SetupSession):
        super().__init__(timeout=900)
        self.session = session
        self.add_item(Option1CategorySelect(session))

        if session.option_2_name:
            self.add_item(Option2CategorySelect(session))

        if session.option_3_name:
            self.add_item(Option3CategorySelect(session))


class SetupHubView(discord.ui.View):
    def __init__(self, session: SetupSession):
        super().__init__(timeout=900)
        self.session = session

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.session.user_id:
            await interaction.response.send_message("This setup is not yours.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Channels / Role", style=discord.ButtonStyle.secondary, row=0)
    async def channels_role_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "Choose the panel channel, log channel and support team role below.",
            view=ChannelsRoleView(self.session),
            ephemeral=True
        )

    @discord.ui.button(label="Categories", style=discord.ButtonStyle.secondary, row=0)
    async def categories_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "Choose the category for each ticket option below.",
            view=CategoriesView(self.session),
            ephemeral=True
        )

    @discord.ui.button(label="Option 3 Name", style=discord.ButtonStyle.secondary, row=0)
    async def option3_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(Option3NameModal(self.session))

    @discord.ui.button(label="Refresh Summary", style=discord.ButtonStyle.primary, row=1)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            embed=build_setup_summary_embed(interaction.guild, self.session),
            view=SetupHubView(self.session),
            ephemeral=True
        )

    @discord.ui.button(label="Publish Panel", style=discord.ButtonStyle.success, row=1)
    async def publish_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        session = self.session

        if not session.title or not session.description or not session.color_hex:
            await interaction.response.send_message("Base setup is incomplete.", ephemeral=True)
            return

        if not session.option_1_name:
            await interaction.response.send_message("Option 1 name is required.", ephemeral=True)
            return

        if not session.option_1_category_id:
            await interaction.response.send_message("Option 1 category is required.", ephemeral=True)
            return

        if session.option_2_name and not session.option_2_category_id:
            await interaction.response.send_message("Option 2 category is required.", ephemeral=True)
            return

        if session.option_3_name and not session.option_3_category_id:
            await interaction.response.send_message("Option 3 category is required.", ephemeral=True)
            return

        if not session.panel_channel_id or not session.log_channel_id or not session.support_role_id:
            await interaction.response.send_message(
                "Panel channel, log channel and support role must all be set.",
                ephemeral=True
            )
            return

        panel_channel = guild.get_channel(session.panel_channel_id)
        log_channel = guild.get_channel(session.log_channel_id)
        support_role = guild.get_role(session.support_role_id)

        if not isinstance(panel_channel, discord.TextChannel):
            await interaction.response.send_message("Panel channel is invalid.", ephemeral=True)
            return

        if not isinstance(log_channel, discord.TextChannel):
            await interaction.response.send_message("Log channel is invalid.", ephemeral=True)
            return

        if not support_role:
            await interaction.response.send_message("Support role is invalid.", ephemeral=True)
            return

        clear_ticket_options(guild.id)
        save_ticket_option(guild.id, 1, session.option_1_name, session.option_1_category_id)

        if session.option_2_name and session.option_2_category_id:
            save_ticket_option(guild.id, 2, session.option_2_name, session.option_2_category_id)

        if session.option_3_name and session.option_3_category_id:
            save_ticket_option(guild.id, 3, session.option_3_name, session.option_3_category_id)

        embed = discord.Embed(
            title=session.title,
            description=session.description,
            color=hex_to_color(session.color_hex)
        )
        embed.set_image(url=session.banner_url)
        embed.set_thumbnail(url=session.thumbnail_url)
        embed.set_footer(text="made by @fntsheetz")

        panel_message = await panel_channel.send(
            embed=embed,
            view=TicketPanelView(guild.id)
        )

        save_guild_config(
            guild_id=guild.id,
            panel_channel_id=panel_channel.id,
            panel_message_id=panel_message.id,
            title=session.title,
            description=session.description,
            color_hex=session.color_hex,
            banner_url=session.banner_url,
            thumbnail_url=session.thumbnail_url,
            support_role_id=support_role.id,
            log_channel_id=log_channel.id
        )

        bot.add_view(TicketPanelView(guild.id), message_id=panel_message.id)
        clear_setup_session(guild.id, interaction.user.id)

        await interaction.response.send_message(
            f"Ticket panel created in {panel_channel.mention}.",
            ephemeral=True
        )


# =========================
# PANEL SELECT
# =========================
class TicketDropdown(discord.ui.Select):
    def __init__(self, guild_id: int):
        rows = get_ticket_options(guild_id)
        options = [
            discord.SelectOption(
                label=row["label"][:100],
                value=str(row["option_index"])
            )
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
                "Ticket system is not configured.",
                ephemeral=True
            )
            return

        existing = get_open_ticket_for_user(guild.id, opener.id)
        if existing:
            existing_channel = guild.get_channel(existing["channel_id"])
            if existing_channel:
                await interaction.response.send_message(
                    f"You already have an open ticket: {existing_channel.mention}",
                    ephemeral=True
                )
                return

        selected_index = int(self.values[0])
        rows = get_ticket_options(guild.id)
        selected = next((r for r in rows if r["option_index"] == selected_index), None)

        if not selected:
            await interaction.response.send_message(
                "That option is no longer configured.",
                ephemeral=True
            )
            return

        category = guild.get_channel(selected["category_id"])
        if not isinstance(category, discord.CategoryChannel):
            await interaction.response.send_message(
                "The configured category is invalid.",
                ephemeral=True
            )
            return

        support_role = guild.get_role(config["support_role_id"])
        if not support_role:
            await interaction.response.send_message(
                "The support role is invalid.",
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
                "I do not have permission to create channels in that category.",
                ephemeral=True
            )
            return

        create_ticket_record(ticket_channel.id, guild.id, opener.id, selected["label"])

        ping_text = f"{support_role.mention} {opener.mention}"
        embed = build_ticket_embed(guild.id, selected["label"], opener)

        await ticket_channel.send(
            content=ping_text,
            embed=embed,
            view=TicketControlView()
        )

        await send_log(
            guild,
            title="Ticket Opened",
            description=(
                f"User: {opener.mention}\n"
                f"Channel: {ticket_channel.mention}\n"
                f"Type: {selected['label']}"
            )
        )

        await interaction.response.send_message(
            f"Your ticket has been created: {ticket_channel.mention}",
            ephemeral=True
        )


class TicketPanelView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.add_item(TicketDropdown(guild_id))


# =========================
# TICKET CONTROLS
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
                "Only the support team or admins can claim tickets.",
                ephemeral=True
            )
            return

        ticket = get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            await interaction.response.send_message(
                "This is not a tracked ticket channel.",
                ephemeral=True
            )
            return

        if ticket["status"] != "open":
            await interaction.response.send_message(
                "This ticket is already closed.",
                ephemeral=True
            )
            return

        if ticket["claimed_by"] == interaction.user.id:
            await interaction.response.send_message(
                "You already claimed this ticket.",
                ephemeral=True
            )
            return

        set_ticket_claimed(interaction.channel.id, interaction.user.id)

        await interaction.response.send_message(
            f"Ticket claimed by {interaction.user.mention}."
        )

        await send_log(
            interaction.guild,
            title="Ticket Claimed",
            description=(
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
                "Only the support team or admins can close tickets.",
                ephemeral=True
            )
            return

        ticket = get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            await interaction.response.send_message(
                "This is not a tracked ticket channel.",
                ephemeral=True
            )
            return

        if ticket["status"] == "closed":
            await interaction.response.send_message(
                "This ticket is already closed.",
                ephemeral=True
            )
            return

        opener = interaction.guild.get_member(ticket["opener_id"])
        config = get_guild_config(interaction.guild.id)
        support_role = interaction.guild.get_role(config["support_role_id"]) if config else None

        try:
            if opener:
                await interaction.channel.set_permissions(opener, view_channel=False)
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
                "I do not have permission to update channel permissions.",
                ephemeral=True
            )
            return

        close_ticket_record(interaction.channel.id)

        await interaction.response.send_message(
            f"Ticket closed by {interaction.user.mention}. Only support team/admins can see it now."
        )

        await send_log(
            interaction.guild,
            title="Ticket Closed",
            description=(
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
                "Only the support team or admins can delete tickets.",
                ephemeral=True
            )
            return

        ticket = get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            await interaction.response.send_message(
                "This is not a tracked ticket channel.",
                ephemeral=True
            )
            return

        transcript_text = await build_transcript_text(interaction.channel)
        transcript_bytes = transcript_text.encode("utf-8", errors="ignore")
        transcript_file = discord.File(
            io.BytesIO(transcript_bytes),
            filename=f"transcript-{interaction.channel.name}.txt"
        )

        opener = interaction.guild.get_member(ticket["opener_id"])
        opener_text = opener.mention if opener else f"<@{ticket['opener_id']}>"

        await send_log(
            interaction.guild,
            title="Ticket Deleted",
            description=(
                f"Channel: #{interaction.channel.name}\n"
                f"Opened by: {opener_text}\n"
                f"Type: {ticket['option_label']}\n"
                f"Deleted by: {interaction.user.mention}"
            ),
            file=transcript_file
        )

        delete_ticket_record(interaction.channel.id)

        await interaction.response.send_message("Deleting ticket...")
        try:
            await interaction.channel.delete(reason=f"Ticket deleted by {interaction.user}")
        except discord.Forbidden:
            pass


class TicketControlView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(ClaimTicketButton())
        self.add_item(CloseTicketButton())
        self.add_item(DeleteTicketButton())


# =========================
# COMMANDS
# =========================
@bot.tree.command(name="setup", description="Open the ticket setup form")
@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
async def setup(
    interaction: discord.Interaction,
    banner: discord.Attachment,
    small_picture: discord.Attachment
):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message(
            "This command can only be used in a server.",
            ephemeral=True
        )
        return

    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            "Only users with Administrator can use this command.",
            ephemeral=True
        )
        return

    if not is_image_attachment(banner):
        await interaction.response.send_message(
            "The banner attachment must be an image.",
            ephemeral=True
        )
        return

    if not is_image_attachment(small_picture):
        await interaction.response.send_message(
            "The small picture attachment must be an image.",
            ephemeral=True
        )
        return

    session = SetupSession(
        guild_id=interaction.guild.id,
        user_id=interaction.user.id,
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
            "This command must be used inside a ticket channel.",
            ephemeral=True
        )
        return

    if not is_support_or_admin(interaction.user, interaction.guild.id):
        await interaction.response.send_message(
            "Only the support team or admins can use this command.",
            ephemeral=True
        )
        return

    ticket = get_ticket_by_channel(interaction.channel.id)
    if not ticket:
        await interaction.response.send_message(
            "This command can only be used inside a ticket channel.",
            ephemeral=True
        )
        return

    if ticket["opener_id"] != user.id:
        await interaction.response.send_message(
            "That user is not the opener of this ticket.",
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
            "I could not DM that user.",
            ephemeral=True
        )
        return

    await interaction.response.send_message(
        f"Reminder sent to {user.mention}.",
        ephemeral=True
    )

    await send_log(
        interaction.guild,
        title="Ticket Reminder Sent",
        description=(
            f"Channel: {interaction.channel.mention}\n"
            f"To: {user.mention}\n"
            f"By: {interaction.user.mention}\n"
            f"Message: {message}"
        )
    )


@setup.error
async def setup_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    msg = f"Setup failed: {error}"
    if interaction.response.is_done():
        await interaction.followup.send(msg, ephemeral=True)
    else:
        await interaction.response.send_message(msg, ephemeral=True)


@remind.error
async def remind_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    msg = f"Remind failed: {error}"
    if interaction.response.is_done():
        await interaction.followup.send(msg, ephemeral=True)
    else:
        await interaction.response.send_message(msg, ephemeral=True)


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

    bot.add_view(TicketControlView())

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
