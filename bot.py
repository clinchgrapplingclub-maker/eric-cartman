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
intents.message_content = True

bot = commands.Bot(command_prefix=commands.when_mentioned, intents=intents)

DB_PATH = "ticketbot.db"

SKIP_WORDS = {"skip", "none", "no", "-"}
CANCEL_WORDS = {"cancel", "stop", "abort", "exit"}


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
# SETUP STATE
# =========================
class SetupCancelled(Exception):
    pass


class SetupData:
    def __init__(self, guild_id: int, user_id: int, setup_channel_id: int):
        self.guild_id = guild_id
        self.user_id = user_id
        self.setup_channel_id = setup_channel_id

        self.title: Optional[str] = None
        self.description: Optional[str] = None
        self.color_hex: str = "#00FF66"

        self.panel_channel_id: Optional[int] = None
        self.support_role_id: Optional[int] = None

        self.option_1_name: Optional[str] = None
        self.option_1_category_id: Optional[int] = None

        self.option_2_name: Optional[str] = None
        self.option_2_category_id: Optional[int] = None

        self.option_3_name: Optional[str] = None
        self.option_3_category_id: Optional[int] = None

        self.log_channel_id: Optional[int] = None
        self.banner_url: Optional[str] = None
        self.thumbnail_url: Optional[str] = None


active_setup_guilds: set[int] = set()
active_setup_users: set[tuple[int, int]] = set()
setup_sessions: dict[tuple[int, int], SetupData] = {}


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


def extract_id(text: str) -> Optional[int]:
    match = re.search(r"(\d{15,25})", text)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def base_embed(
    guild_id: Optional[int],
    title: str,
    description: str,
    *,
    error: bool = False
) -> discord.Embed:
    if error:
        color = discord.Color.red()
    else:
        config = get_guild_config(guild_id) if guild_id else None
        color = hex_to_color(config["color_hex"]) if config else discord.Color.green()

    embed = discord.Embed(title=title, description=description, color=color)
    embed.set_footer(text="made by @fntsheetz")
    return embed


def setup_embed(data: SetupData, title: str, description: str) -> discord.Embed:
    embed = discord.Embed(
        title=title,
        description=description,
        color=hex_to_color(data.color_hex if data.color_hex else "#00FF66")
    )
    embed.set_footer(text="made by @fntsheetz")
    return embed


def build_panel_embed(guild_id: int) -> discord.Embed:
    config = get_guild_config(guild_id)
    if not config:
        return base_embed(
            None,
            "Ticket panel not configured",
            "This server has not configured the ticket panel yet.",
            error=True
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


def build_closed_ticket_embed(guild_id: int, closed_by: discord.Member) -> discord.Embed:
    config = get_guild_config(guild_id)
    color = hex_to_color(config["color_hex"]) if config else discord.Color.green()

    embed = discord.Embed(
        title="Ticket Closed",
        description=f"Ticket closed by {closed_by.mention}.",
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


def resolve_text_channel(guild: discord.Guild, raw: str) -> Optional[discord.TextChannel]:
    cid = extract_id(raw)
    if cid:
        ch = guild.get_channel(cid)
        if isinstance(ch, discord.TextChannel):
            return ch

    raw_clean = raw.strip().replace("#", "")
    for ch in guild.text_channels:
        if ch.name.lower() == raw_clean.lower():
            return ch
    return None


def resolve_category(guild: discord.Guild, raw: str) -> Optional[discord.CategoryChannel]:
    cid = extract_id(raw)
    if cid:
        ch = guild.get_channel(cid)
        if isinstance(ch, discord.CategoryChannel):
            return ch

    raw_clean = raw.strip().replace("#", "")
    for ch in guild.categories:
        if ch.name.lower() == raw_clean.lower():
            return ch
    return None


def resolve_role(guild: discord.Guild, raw: str) -> Optional[discord.Role]:
    rid = extract_id(raw)
    if rid:
        role = guild.get_role(rid)
        if role:
            return role

    raw_clean = raw.strip().replace("@", "")
    for role in guild.roles:
        if role.name.lower() == raw_clean.lower():
            return role
    return None


async def safe_delete(message: Optional[discord.Message]):
    if not message:
        return
    try:
        await message.delete()
    except Exception:
        pass


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


def build_setup_preview_embed(guild: discord.Guild, data: SetupData) -> discord.Embed:
    color = hex_to_color(data.color_hex if data.color_hex else "#00FF66")

    panel_channel = guild.get_channel(data.panel_channel_id) if data.panel_channel_id else None
    support_role = guild.get_role(data.support_role_id) if data.support_role_id else None
    log_channel = guild.get_channel(data.log_channel_id) if data.log_channel_id else None

    cat1 = guild.get_channel(data.option_1_category_id) if data.option_1_category_id else None
    cat2 = guild.get_channel(data.option_2_category_id) if data.option_2_category_id else None
    cat3 = guild.get_channel(data.option_3_category_id) if data.option_3_category_id else None

    embed = discord.Embed(
        title=data.title or "Setup Preview",
        description=data.description or "No description set.",
        color=color
    )

    embed.add_field(
        name="Panel Settings",
        value=(
            f"**Color:** `{data.color_hex}`\n"
            f"**Panel Channel:** {panel_channel.mention if panel_channel else '`Not set`'}\n"
            f"**Support Team:** {support_role.mention if support_role else '`Not set`'}\n"
            f"**Log Channel:** {log_channel.mention if log_channel else '`Not set`'}"
        ),
        inline=False
    )

    embed.add_field(
        name="Ticket Options",
        value=(
            f"**1.** {data.option_1_name or '`Not set`'} - {cat1.name if cat1 else '`Not set`'}\n"
            f"**2.** {(data.option_2_name or '`Skipped`')} - {(cat2.name if cat2 else ('`Skipped`' if not data.option_2_name else '`Not set`'))}\n"
            f"**3.** {(data.option_3_name or '`Skipped`')} - {(cat3.name if cat3 else ('`Skipped`' if not data.option_3_name else '`Not set`'))}"
        ),
        inline=False
    )

    if data.thumbnail_url:
        embed.set_thumbnail(url=data.thumbnail_url)

    if data.banner_url:
        embed.set_image(url=data.banner_url)

    embed.set_footer(text="made by @fntsheetz")
    return embed


# =========================
# SETUP QUESTION FLOW
# =========================
async def wait_for_user_message(channel: discord.TextChannel, user: discord.Member, timeout: int = 300) -> discord.Message:
    def check(m: discord.Message):
        return (
            m.author.id == user.id
            and m.channel.id == channel.id
            and not m.author.bot
        )
    return await bot.wait_for("message", check=check, timeout=timeout)


async def ask_text(
    channel: discord.TextChannel,
    user: discord.Member,
    data: SetupData,
    title: str,
    description: str,
    *,
    optional: bool = False,
    multiline: bool = False,
) -> Optional[str]:
    prompt = await channel.send(embed=setup_embed(data, title, description))
    reply = None
    try:
        reply = await wait_for_user_message(channel, user)
        content = reply.content.strip()

        if content.lower() in CANCEL_WORDS:
            raise SetupCancelled()

        if optional and content.lower() in SKIP_WORDS:
            return None

        if not content:
            if optional:
                return None
            raise ValueError("Empty answer.")

        return content
    finally:
        await safe_delete(prompt)
        await safe_delete(reply)


async def ask_role(
    channel: discord.TextChannel,
    user: discord.Member,
    data: SetupData,
    guild: discord.Guild,
    title: str,
    description: str,
) -> discord.Role:
    while True:
        raw = await ask_text(channel, user, data, title, description)
        role = resolve_role(guild, raw)
        if role:
            return role

        await channel.send(
            embed=setup_embed(
                data,
                "Invalid Role",
                "Could not find that role. Send a role mention, role ID, or exact role name."
            ),
            delete_after=8
        )


async def ask_text_channel(
    channel: discord.TextChannel,
    user: discord.Member,
    data: SetupData,
    guild: discord.Guild,
    title: str,
    description: str,
) -> discord.TextChannel:
    while True:
        raw = await ask_text(channel, user, data, title, description)
        resolved = resolve_text_channel(guild, raw)
        if resolved:
            return resolved

        await channel.send(
            embed=setup_embed(
                data,
                "Invalid Channel",
                "Could not find that text channel. Send a channel mention, channel ID, or exact channel name."
            ),
            delete_after=8
        )


async def ask_category(
    channel: discord.TextChannel,
    user: discord.Member,
    data: SetupData,
    guild: discord.Guild,
    title: str,
    description: str,
    *,
    optional: bool = False,
) -> Optional[discord.CategoryChannel]:
    while True:
        raw = await ask_text(channel, user, data, title, description, optional=optional)
        if raw is None:
            return None

        resolved = resolve_category(guild, raw)
        if resolved:
            return resolved

        await channel.send(
            embed=setup_embed(
                data,
                "Invalid Category",
                "Could not find that category. Send the category ID or exact category name."
            ),
            delete_after=8
        )


async def ask_image(
    channel: discord.TextChannel,
    user: discord.Member,
    data: SetupData,
    title: str,
    description: str,
) -> str:
    prompt = await channel.send(embed=setup_embed(data, title, description))
    reply = None

    try:
        reply = await wait_for_user_message(channel, user)

        if reply.content.strip().lower() in CANCEL_WORDS:
            raise SetupCancelled()

        if not reply.attachments:
            await channel.send(
                embed=setup_embed(
                    data,
                    "No Image Found",
                    "You need to upload an image in your reply."
                ),
                delete_after=8
            )
            return await ask_image(channel, user, data, title, description)

        attachment = reply.attachments[0]
        if not is_image_attachment(attachment):
            await channel.send(
                embed=setup_embed(
                    data,
                    "Invalid Image",
                    "Attachment must be an image."
                ),
                delete_after=8
            )
            return await ask_image(channel, user, data, title, description)

        return attachment.url
    finally:
        await safe_delete(prompt)
        await safe_delete(reply)


async def ask_option_name_and_category(
    channel: discord.TextChannel,
    user: discord.Member,
    data: SetupData,
    guild: discord.Guild,
    number: int,
    *,
    optional: bool = False,
) -> tuple[Optional[str], Optional[int]]:
    prompt_title = f"Ticket Category {number}"
    prompt_description = (
        "Reply in this format:\n"
        "`Ticket Name | Category Name or Category ID`\n\n"
        "Example:\n"
        "`Support Ticket | tickets`\n"
        "`Purchase Ticket | 123456789012345678`\n\n"
    )
    if optional:
        prompt_description += "Type `skip` if you do not want this option."

    while True:
        raw = await ask_text(
            channel,
            user,
            data,
            prompt_title,
            prompt_description,
            optional=optional
        )
        if raw is None:
            return None, None

        if "|" not in raw:
            await channel.send(
                embed=setup_embed(
                    data,
                    "Invalid Format",
                    "Use this exact format:\n`Ticket Name | Category Name or Category ID`"
                ),
                delete_after=8
            )
            continue

        name_part, category_part = raw.split("|", 1)
        name = name_part.strip()
        category_raw = category_part.strip()

        if not name or not category_raw:
            await channel.send(
                embed=setup_embed(
                    data,
                    "Invalid Format",
                    "Both ticket name and category are required."
                ),
                delete_after=8
            )
            continue

        category = resolve_category(guild, category_raw)
        if not category:
            await channel.send(
                embed=setup_embed(
                    data,
                    "Invalid Category",
                    "Could not find that category. Use exact category name or category ID."
                ),
                delete_after=8
            )
            continue

        return name, category.id


async def run_setup_wizard(interaction: discord.Interaction):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return

    guild = interaction.guild
    user = interaction.user
    channel = interaction.channel

    if not isinstance(channel, discord.TextChannel):
        await interaction.followup.send(
            embed=base_embed(
                guild.id,
                "Setup Failed",
                "Setup must be run in a text channel.",
                error=True
            ),
            ephemeral=True
        )
        return

    data = SetupData(guild.id, user.id, channel.id)
    setup_sessions[(guild.id, user.id)] = data

    try:
        await channel.send(
            embed=setup_embed(
                data,
                "Ticket Setup Started",
                "I will ask you one question at a time.\n\n"
                "Reply in this channel.\n"
                "Type `cancel` anytime to stop the setup."
            )
        )

        data.title = await ask_text(
            channel, user, data,
            "Title",
            "Send the ticket panel title."
        )

        data.description = await ask_text(
            channel, user, data,
            "Description",
            "Send the ticket panel description.",
            multiline=True
        )

        color_raw = await ask_text(
            channel, user, data,
            "Embed Color",
            "Send the embed hex color.\nExample: `#00FF66`\n\nType `skip` to use the default color.",
            optional=True
        )
        if color_raw:
            try:
                data.color_hex = normalize_hex(color_raw)
            except ValueError:
                await channel.send(
                    embed=base_embed(
                        None,
                        "Invalid Color",
                        "Invalid hex color. Default color `#00FF66` will be used.",
                        error=True
                    ),
                    delete_after=8
                )
                data.color_hex = "#00FF66"

        panel_channel = await ask_text_channel(
            channel, user, data, guild,
            "Panel Channel",
            "Send the panel channel mention, channel ID, or exact channel name."
        )
        data.panel_channel_id = panel_channel.id

        support_role = await ask_role(
            channel, user, data, guild,
            "Support Team Role",
            "Send the support role mention, role ID, or exact role name."
        )
        data.support_role_id = support_role.id

        data.option_1_name, data.option_1_category_id = await ask_option_name_and_category(
            channel, user, data, guild, 1, optional=False
        )

        data.option_2_name, data.option_2_category_id = await ask_option_name_and_category(
            channel, user, data, guild, 2, optional=True
        )

        data.option_3_name, data.option_3_category_id = await ask_option_name_and_category(
            channel, user, data, guild, 3, optional=True
        )

        log_channel = await ask_text_channel(
            channel, user, data, guild,
            "Log Channel",
            "Send the log channel mention, channel ID, or exact channel name."
        )
        data.log_channel_id = log_channel.id

        data.banner_url = await ask_image(
            channel, user, data,
            "Banner",
            "Reply with the banner image uploaded as an attachment."
        )

        data.thumbnail_url = await ask_image(
            channel, user, data,
            "Small Picture",
            "Reply with the small picture image uploaded as an attachment."
        )

        preview_message = await channel.send(
            embed=build_setup_preview_embed(guild, data),
            view=SetupConfirmView(data)
        )

        setup_sessions[(guild.id, user.id)] = data

        await channel.send(
            embed=setup_embed(
                data,
                "Setup Ready",
                "Review the preview above and click **Publish** or **Cancel**."
            ),
            delete_after=20
        )

    except SetupCancelled:
        await channel.send(
            embed=setup_embed(
                data,
                "Setup Cancelled",
                "The ticket setup was cancelled."
            )
        )
        cleanup_setup(guild.id, user.id)

    except Exception as e:
        await channel.send(
            embed=base_embed(
                guild.id,
                "Setup Failed",
                f"An error happened during setup:\n`{e}`",
                error=True
            )
        )
        cleanup_setup(guild.id, user.id)


def cleanup_setup(guild_id: int, user_id: int):
    active_setup_guilds.discard(guild_id)
    active_setup_users.discard((guild_id, user_id))
    setup_sessions.pop((guild_id, user_id), None)


# =========================
# SETUP CONFIRM VIEW
# =========================
class SetupConfirmView(discord.ui.View):
    def __init__(self, data: SetupData):
        super().__init__(timeout=900)
        self.data = data

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.data.user_id:
            await interaction.response.send_message(
                embed=base_embed(
                    interaction.guild.id if interaction.guild else None,
                    "Access Denied",
                    "This setup is not yours.",
                    error=True
                ),
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Publish", style=discord.ButtonStyle.success)
    async def publish_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild:
            return

        data = self.data
        guild = interaction.guild

        clear_ticket_options(guild.id)
        save_ticket_option(guild.id, 1, data.option_1_name, data.option_1_category_id)

        if data.option_2_name and data.option_2_category_id:
            save_ticket_option(guild.id, 2, data.option_2_name, data.option_2_category_id)

        if data.option_3_name and data.option_3_category_id:
            save_ticket_option(guild.id, 3, data.option_3_name, data.option_3_category_id)

        panel_channel = guild.get_channel(data.panel_channel_id)
        if not isinstance(panel_channel, discord.TextChannel):
            await interaction.response.send_message(
                embed=base_embed(guild.id, "Publish Failed", "Panel channel is invalid.", error=True),
                ephemeral=True
            )
            return

        embed = discord.Embed(
            title=data.title,
            description=data.description,
            color=hex_to_color(data.color_hex)
        )
        embed.set_thumbnail(url=data.thumbnail_url)
        embed.set_image(url=data.banner_url)
        embed.set_footer(text="made by @fntsheetz")

        panel_message = await panel_channel.send(
            embed=embed,
            view=TicketPanelView(guild.id)
        )

        save_guild_config(
            guild_id=guild.id,
            panel_channel_id=data.panel_channel_id,
            panel_message_id=panel_message.id,
            title=data.title,
            description=data.description,
            color_hex=data.color_hex,
            banner_url=data.banner_url,
            thumbnail_url=data.thumbnail_url,
            support_role_id=data.support_role_id,
            log_channel_id=data.log_channel_id
        )

        bot.add_view(TicketPanelView(guild.id), message_id=panel_message.id)

        await interaction.response.edit_message(
            embed=base_embed(
                guild.id,
                "Setup Complete",
                f"Ticket panel created in {panel_channel.mention}."
            ),
            view=None
        )

        cleanup_setup(guild.id, interaction.user.id)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            embed=base_embed(
                interaction.guild.id if interaction.guild else None,
                "Setup Cancelled",
                "The setup was cancelled."
            ),
            view=None
        )
        if interaction.guild:
            cleanup_setup(interaction.guild.id, interaction.user.id)


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
                embed=base_embed(guild.id, "Error", "Ticket system is not configured.", error=True),
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
                embed=base_embed(guild.id, "Error", "That ticket option is no longer configured.", error=True),
                ephemeral=True
            )
            return

        category = guild.get_channel(selected["category_id"])
        if not isinstance(category, discord.CategoryChannel):
            await interaction.response.send_message(
                embed=base_embed(guild.id, "Error", "The configured category is invalid.", error=True),
                ephemeral=True
            )
            return

        support_role = guild.get_role(config["support_role_id"])
        if not support_role:
            await interaction.response.send_message(
                embed=base_embed(guild.id, "Error", "The support role is invalid.", error=True),
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
                    error=True
                ),
                ephemeral=True
            )
            return

        create_ticket_record(ticket_channel.id, guild.id, opener.id, selected["label"])

        await ticket_channel.send(
            content=f"{support_role.mention} {opener.mention}",
            embed=build_ticket_embed(guild.id, selected["label"], opener),
            view=TicketControlsView()
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
            embed=base_embed(guild.id, "Ticket Created", f"Your ticket has been created: {ticket_channel.mention}"),
            ephemeral=True
        )


class TicketPanelView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.add_item(TicketDropdown(guild_id))


# =========================
# TICKET BUTTONS
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
                embed=base_embed(interaction.guild.id, "Access Denied", "Only the support team or admins can claim tickets.", error=True),
                ephemeral=True
            )
            return

        ticket = get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            await interaction.response.send_message(
                embed=base_embed(interaction.guild.id, "Error", "This is not a tracked ticket channel.", error=True),
                ephemeral=True
            )
            return

        if ticket["claimed_by"] == interaction.user.id:
            await interaction.response.send_message(
                embed=base_embed(interaction.guild.id, "Already Claimed", "You already claimed this ticket."),
                ephemeral=True
            )
            return

        set_ticket_claimed(interaction.channel.id, interaction.user.id)

        await interaction.response.send_message(
            embed=base_embed(interaction.guild.id, "Ticket Claimed", f"Ticket claimed by {interaction.user.mention}.")
        )

        await send_log(
            interaction.guild,
            "Ticket Claimed",
            f"Channel: {interaction.channel.mention}\nClaimed by: {interaction.user.mention}"
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
                embed=base_embed(interaction.guild.id, "Access Denied", "Only the support team or admins can close tickets.", error=True),
                ephemeral=True
            )
            return

        ticket = get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            await interaction.response.send_message(
                embed=base_embed(interaction.guild.id, "Error", "This is not a tracked ticket channel.", error=True),
                ephemeral=True
            )
            return

        if ticket["status"] == "closed":
            await interaction.response.send_message(
                embed=base_embed(interaction.guild.id, "Already Closed", "This ticket is already closed."),
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
                embed=base_embed(interaction.guild.id, "Error", "I do not have permission to update channel permissions.", error=True),
                ephemeral=True
            )
            return

        close_ticket_record(interaction.channel.id)

        await interaction.response.edit_message(
            content=None,
            embed=build_closed_ticket_embed(interaction.guild.id, interaction.user),
            view=TicketControlsView()
        )

        await send_log(
            interaction.guild,
            "Ticket Closed",
            f"Channel: #{interaction.channel.name}\nClosed by: {interaction.user.mention}"
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
                embed=base_embed(interaction.guild.id, "Access Denied", "Only the support team or admins can delete tickets.", error=True),
                ephemeral=True
            )
            return

        ticket = get_ticket_by_channel(interaction.channel.id)
        if not ticket:
            await interaction.response.send_message(
                embed=base_embed(interaction.guild.id, "Error", "This is not a tracked ticket channel.", error=True),
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
            embed=base_embed(interaction.guild.id, "Deleting Ticket", "The ticket is being deleted.")
        )

        try:
            await interaction.channel.delete(reason=f"Ticket deleted by {interaction.user}")
        except discord.Forbidden:
            pass


class TicketControlsView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(ClaimTicketButton())
        self.add_item(CloseTicketButton())
        self.add_item(DeleteTicketButton())


# =========================
# COMMANDS
# =========================
@bot.tree.command(name="setup", description="Start the guided ticket setup")
@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
async def setup(interaction: discord.Interaction):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return

    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            embed=base_embed(None, "Access Denied", "Only users with Administrator can use this command.", error=True),
            ephemeral=True
        )
        return

    if interaction.guild.id in active_setup_guilds:
        await interaction.response.send_message(
            embed=base_embed(interaction.guild.id, "Setup Running", "A setup is already running in this server."),
            ephemeral=True
        )
        return

    active_setup_guilds.add(interaction.guild.id)
    active_setup_users.add((interaction.guild.id, interaction.user.id))

    await interaction.response.send_message(
        embed=base_embed(
            interaction.guild.id,
            "Setup Started",
            "The guided setup has started in this channel."
        ),
        ephemeral=True
    )

    await run_setup_wizard(interaction)


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
            embed=base_embed(interaction.guild.id, "Error", "This command must be used inside a ticket channel.", error=True),
            ephemeral=True
        )
        return

    if not is_support_or_admin(interaction.user, interaction.guild.id):
        await interaction.response.send_message(
            embed=base_embed(interaction.guild.id, "Access Denied", "Only the support team or admins can use this command.", error=True),
            ephemeral=True
        )
        return

    ticket = get_ticket_by_channel(interaction.channel.id)
    if not ticket:
        await interaction.response.send_message(
            embed=base_embed(interaction.guild.id, "Error", "This command can only be used inside a ticket channel.", error=True),
            ephemeral=True
        )
        return

    if ticket["opener_id"] != user.id:
        await interaction.response.send_message(
            embed=base_embed(interaction.guild.id, "Error", "That user is not the opener of this ticket.", error=True),
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
            embed=base_embed(interaction.guild.id, "DM Failed", "I could not DM that user.", error=True),
            ephemeral=True
        )
        return

    await interaction.response.send_message(
        embed=base_embed(interaction.guild.id, "Reminder Sent", f"Reminder sent to {user.mention}."),
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
            embed=base_embed(None, "Access Denied", "Only users with Administrator can use this command.", error=True),
            ephemeral=True
        )
        return

    if avatar is not None and not is_image_attachment(avatar):
        await interaction.response.send_message(
            embed=base_embed(None, "Update Failed", "Avatar attachment must be an image.", error=True),
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
            embed=base_embed(interaction.guild.id, "Server Profile Updated", "The bot profile was updated for this server."),
            ephemeral=True
        )
    else:
        await interaction.response.send_message(
            embed=base_embed(interaction.guild.id, "Update Failed", message, error=True),
            ephemeral=True
        )


@setup.error
async def setup_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if interaction.guild:
        cleanup_setup(interaction.guild.id, interaction.user.id)

    embed = base_embed(
        interaction.guild.id if interaction.guild else None,
        "Setup Failed",
        f"{error}",
        error=True
    )

    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


@remind.error
async def remind_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    embed = base_embed(
        interaction.guild.id if interaction.guild else None,
        "Remind Failed",
        f"{error}",
        error=True
    )

    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


@serverprofile.error
async def serverprofile_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    embed = base_embed(
        interaction.guild.id if interaction.guild else None,
        "Update Failed",
        f"{error}",
        error=True
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

    bot.add_view(TicketControlsView())

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
