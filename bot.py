import aiohttp
import asyncio
import base64
import html
import io
import logging
import os
import re
import zipfile
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Any

import asyncpg
import discord
from discord import app_commands
from discord.ext import commands


# =========================================================
# ENV / LOGGING
# =========================================================
TOKEN = os.getenv("DISCORD_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is not set in environment variables.")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set in environment variables.")

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)-8s] %(name)s: %(message)s"
)
log = logging.getLogger("ticketbot")


# =========================================================
# DISCORD BOT
# =========================================================
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix=commands.when_mentioned, intents=intents)

SKIP_WORDS = {"skip", "none", "no", "-"}
CANCEL_WORDS = {"cancel", "stop", "abort", "exit"}

db_pool: Optional[asyncpg.Pool] = None

db_init_lock = asyncio.Lock()
active_setup_guilds: set[int] = set()

ticket_channel_locks: dict[int, asyncio.Lock] = {}
ticket_create_locks: dict[tuple[int, int], asyncio.Lock] = {}

setup_sessions: dict[tuple[int, int], "SetupData"] = {}

guild_config_cache: dict[int, dict[str, Any]] = {}
ticket_options_cache: dict[int, list[dict[str, Any]]] = {}

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=60)


# =========================================================
# LOCK HELPERS
# =========================================================
def get_ticket_channel_lock(channel_id: int) -> asyncio.Lock:
    lock = ticket_channel_locks.get(channel_id)
    if lock is None:
        lock = asyncio.Lock()
        ticket_channel_locks[channel_id] = lock
    return lock


def get_ticket_create_lock(guild_id: int, user_id: int) -> asyncio.Lock:
    key = (guild_id, user_id)
    lock = ticket_create_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        ticket_create_locks[key] = lock
    return lock


# =========================================================
# DATA CLASSES / STATE
# =========================================================
class SetupCancelled(Exception):
    pass


@dataclass
class SetupData:
    guild_id: int
    user_id: int
    setup_channel_id: int

    title: Optional[str] = None
    description: Optional[str] = None
    color_hex: str = "#00FF66"

    panel_channel_id: Optional[int] = None
    support_role_id: Optional[int] = None

    option_1_name: Optional[str] = None
    option_1_category_id: Optional[int] = None

    option_2_name: Optional[str] = None
    option_2_category_id: Optional[int] = None

    option_3_name: Optional[str] = None
    option_3_category_id: Optional[int] = None

    log_channel_id: Optional[int] = None
    banner_url: Optional[str] = None
    thumbnail_url: Optional[str] = None


@dataclass
class ImageData:
    filename: str
    mime_type: str
    raw: bytes


def cleanup_setup(guild_id: int, user_id: int):
    active_setup_guilds.discard(guild_id)
    setup_sessions.pop((guild_id, user_id), None)


# =========================================================
# DATABASE
# =========================================================
async def create_db_pool():
    global db_pool

    if db_pool is not None:
        return

    db_pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=1,
        max_size=10,
        command_timeout=30,
        statement_cache_size=0,
        max_inactive_connection_lifetime=300
    )
    log.info("Postgres pool created.")


async def init_db():
    async with db_init_lock:
        await create_db_pool()
        assert db_pool is not None

        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS guild_config (
                    guild_id BIGINT PRIMARY KEY,
                    panel_channel_id BIGINT NOT NULL,
                    panel_message_id BIGINT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT NOT NULL,
                    color_hex TEXT NOT NULL,
                    banner_url TEXT,
                    thumbnail_url TEXT,
                    support_role_id BIGINT NOT NULL,
                    log_channel_id BIGINT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS ticket_options (
                    guild_id BIGINT NOT NULL,
                    option_index INTEGER NOT NULL,
                    label TEXT NOT NULL,
                    category_id BIGINT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (guild_id, option_index)
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS tickets (
                    channel_id BIGINT PRIMARY KEY,
                    guild_id BIGINT NOT NULL,
                    opener_id BIGINT NOT NULL,
                    option_label TEXT NOT NULL,
                    status TEXT NOT NULL,
                    claimed_by BIGINT,
                    closed_by BIGINT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    closed_at TIMESTAMPTZ
                )
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tickets_guild_opener_status
                ON tickets(guild_id, opener_id, status)
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tickets_status
                ON tickets(status)
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tickets_guild_status
                ON tickets(guild_id, status)
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_ticket_options_guild
                ON ticket_options(guild_id)
            """)

        log.info("Database initialized.")


async def db_fetchrow(query: str, *args):
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(query, *args)
        return dict(row) if row else None


async def db_fetch(query: str, *args):
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *args)
        return [dict(r) for r in rows]


async def db_execute(query: str, *args):
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        return await conn.execute(query, *args)


async def get_guild_config(guild_id: int) -> Optional[dict[str, Any]]:
    cached = guild_config_cache.get(guild_id)
    if cached is not None:
        return cached

    row = await db_fetchrow("""
        SELECT guild_id, panel_channel_id, panel_message_id, title, description,
               color_hex, banner_url, thumbnail_url, support_role_id, log_channel_id
        FROM guild_config
        WHERE guild_id = $1
    """, guild_id)

    if row:
        guild_config_cache[guild_id] = row
    return row


async def save_guild_config(
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
    await db_execute("""
        INSERT INTO guild_config (
            guild_id, panel_channel_id, panel_message_id, title, description,
            color_hex, banner_url, thumbnail_url, support_role_id, log_channel_id,
            created_at, updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW(),NOW())
        ON CONFLICT (guild_id)
        DO UPDATE SET
            panel_channel_id = EXCLUDED.panel_channel_id,
            panel_message_id = EXCLUDED.panel_message_id,
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            color_hex = EXCLUDED.color_hex,
            banner_url = EXCLUDED.banner_url,
            thumbnail_url = EXCLUDED.thumbnail_url,
            support_role_id = EXCLUDED.support_role_id,
            log_channel_id = EXCLUDED.log_channel_id,
            updated_at = NOW()
    """, guild_id, panel_channel_id, panel_message_id, title, description,
         color_hex, banner_url, thumbnail_url, support_role_id, log_channel_id)

    guild_config_cache[guild_id] = {
        "guild_id": guild_id,
        "panel_channel_id": panel_channel_id,
        "panel_message_id": panel_message_id,
        "title": title,
        "description": description,
        "color_hex": color_hex,
        "banner_url": banner_url,
        "thumbnail_url": thumbnail_url,
        "support_role_id": support_role_id,
        "log_channel_id": log_channel_id
    }


async def clear_ticket_options(guild_id: int):
    await db_execute("DELETE FROM ticket_options WHERE guild_id = $1", guild_id)
    ticket_options_cache.pop(guild_id, None)


async def save_ticket_option(guild_id: int, option_index: int, label: str, category_id: int):
    await db_execute("""
        INSERT INTO ticket_options (guild_id, option_index, label, category_id, created_at, updated_at)
        VALUES ($1,$2,$3,$4,NOW(),NOW())
        ON CONFLICT (guild_id, option_index)
        DO UPDATE SET
            label = EXCLUDED.label,
            category_id = EXCLUDED.category_id,
            updated_at = NOW()
    """, guild_id, option_index, label, category_id)

    ticket_options_cache.pop(guild_id, None)


async def get_ticket_options(guild_id: int) -> list[dict[str, Any]]:
    cached = ticket_options_cache.get(guild_id)
    if cached is not None:
        return cached

    rows = await db_fetch("""
        SELECT guild_id, option_index, label, category_id
        FROM ticket_options
        WHERE guild_id = $1
        ORDER BY option_index ASC
    """, guild_id)

    ticket_options_cache[guild_id] = rows
    return rows


async def create_ticket_record(channel_id: int, guild_id: int, opener_id: int, option_label: str):
    await db_execute("""
        INSERT INTO tickets (
            channel_id, guild_id, opener_id, option_label, status,
            claimed_by, closed_by, created_at, closed_at
        )
        VALUES ($1,$2,$3,$4,'open',NULL,NULL,NOW(),NULL)
        ON CONFLICT (channel_id) DO NOTHING
    """, channel_id, guild_id, opener_id, option_label)


async def get_ticket_by_channel(channel_id: int) -> Optional[dict[str, Any]]:
    return await db_fetchrow("""
        SELECT channel_id, guild_id, opener_id, option_label, status,
               claimed_by, closed_by, created_at, closed_at
        FROM tickets
        WHERE channel_id = $1
    """, channel_id)


async def get_open_ticket_for_user(guild_id: int, opener_id: int) -> Optional[dict[str, Any]]:
    return await db_fetchrow("""
        SELECT channel_id, guild_id, opener_id, option_label, status,
               claimed_by, closed_by, created_at, closed_at
        FROM tickets
        WHERE guild_id = $1
          AND opener_id = $2
          AND status = 'open'
        ORDER BY created_at DESC
        LIMIT 1
    """, guild_id, opener_id)


async def set_ticket_claimed(channel_id: int, claimed_by: int):
    await db_execute("""
        UPDATE tickets
        SET claimed_by = $1
        WHERE channel_id = $2
    """, claimed_by, channel_id)


async def close_ticket_record(channel_id: int, closed_by: Optional[int] = None):
    await db_execute("""
        UPDATE tickets
        SET status = 'closed',
            closed_by = $1,
            closed_at = NOW()
        WHERE channel_id = $2
          AND status <> 'closed'
    """, closed_by, channel_id)


async def delete_ticket_record(channel_id: int):
    await db_execute("DELETE FROM tickets WHERE channel_id = $1", channel_id)


async def get_all_panel_rows() -> list[dict[str, Any]]:
    return await db_fetch("""
        SELECT guild_id, panel_message_id
        FROM guild_config
    """)


# =========================================================
# GENERAL HELPERS
# =========================================================
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


async def try_fetch_member(guild: discord.Guild, user_id: int) -> Optional[discord.Member]:
    member = guild.get_member(user_id)
    if member:
        return member
    with suppress(Exception):
        return await guild.fetch_member(user_id)
    return None


async def try_fetch_user(user_id: int) -> Optional[discord.User]:
    user = bot.get_user(user_id)
    if user:
        return user
    with suppress(Exception):
        return await bot.fetch_user(user_id)
    return None


async def safe_delete_message(message: Optional[discord.Message]):
    if message is None:
        return
    with suppress(Exception):
        await message.delete()


def get_bot_guild_avatar_url(guild_id: Optional[int]) -> Optional[str]:
    if guild_id is None or bot.user is None:
        return None

    guild = bot.get_guild(guild_id)
    if guild is None:
        return bot.user.display_avatar.url

    me = guild.me
    if me:
        return me.display_avatar.url

    return bot.user.display_avatar.url


def format_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


async def resolve_color_for_guild(guild_id: Optional[int], *, error: bool = False) -> discord.Color:
    if error:
        return discord.Color.red()

    if guild_id is None:
        return discord.Color.green()

    config = await get_guild_config(guild_id)
    if config:
        with suppress(Exception):
            return hex_to_color(config["color_hex"])
    return discord.Color.green()


async def base_embed(
    guild_id: Optional[int],
    title: str,
    description: str,
    *,
    error: bool = False
) -> discord.Embed:
    color = await resolve_color_for_guild(guild_id, error=error)
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


async def build_ticket_embed(guild_id: int, option_label: str, opener: discord.Member) -> discord.Embed:
    config = await get_guild_config(guild_id)
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


async def build_closed_ticket_embed(guild_id: int, closed_by: discord.Member) -> discord.Embed:
    config = await get_guild_config(guild_id)
    color = hex_to_color(config["color_hex"]) if config else discord.Color.green()

    embed = discord.Embed(
        title="Ticket Closed",
        description=f"Ticket closed by {closed_by.mention}.",
        color=color
    )
    embed.set_footer(text="made by @fntsheetz")
    return embed


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


async def is_support_or_admin(member: discord.Member, guild_id: int) -> bool:
    if member.guild_permissions.administrator:
        return True

    config = await get_guild_config(guild_id)
    if not config:
        return False

    role = member.guild.get_role(config["support_role_id"])
    return role in member.roles if role else False


async def send_log(
    guild: discord.Guild,
    title: str,
    description: str,
    file: Optional[discord.File] = None
):
    config = await get_guild_config(guild.id)
    if not config:
        return

    log_channel = guild.get_channel(config["log_channel_id"])
    if not isinstance(log_channel, discord.TextChannel):
        return

    embed = await base_embed(guild.id, title, description)

    try:
        if file:
            await log_channel.send(embed=embed, file=file)
        else:
            await log_channel.send(embed=embed)
    except Exception as e:
        log.warning("Failed to send log in guild %s: %s", guild.id, e)


async def safe_defer(interaction: discord.Interaction, *, ephemeral: bool = False, thinking: bool = False) -> bool:
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=ephemeral, thinking=thinking)
            return True
    except Exception:
        return False
    return False


async def safe_component_reply(
    interaction: discord.Interaction,
    *,
    embed: Optional[discord.Embed] = None,
    content: Optional[str] = None,
    ephemeral: bool = False
):
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message(content=content, embed=embed, ephemeral=ephemeral)
            return
    except Exception:
        pass

    try:
        await interaction.followup.send(content=content, embed=embed, ephemeral=ephemeral)
        return
    except Exception:
        pass

    try:
        if interaction.channel and isinstance(interaction.channel, discord.TextChannel):
            await interaction.channel.send(content=content, embed=embed)
    except Exception:
        pass


async def safe_edit_original_response(
    interaction: discord.Interaction,
    *,
    embed: Optional[discord.Embed] = None,
    content: Optional[str] = None,
    view: Optional[discord.ui.View] = None
) -> bool:
    try:
        await interaction.edit_original_response(content=content, embed=embed, view=view)
        return True
    except Exception:
        return False


async def ensure_interaction_ack(interaction: discord.Interaction, *, ephemeral: bool = True):
    if interaction.response.is_done():
        return
    with suppress(Exception):
        await interaction.response.defer(ephemeral=ephemeral, thinking=False)


async def refresh_panel_message(message: Optional[discord.Message], guild_id: int):
    if not message:
        return
    try:
        view = await TicketPanelView.build(guild_id)
        await message.edit(view=view)
    except Exception:
        pass


async def dm_ticket_closed(
    guild: discord.Guild,
    opener_id: int,
    closed_by: discord.Member,
    channel_name: str
):
    config = await get_guild_config(guild.id)
    color = hex_to_color(config["color_hex"]) if config else discord.Color.green()

    user = await try_fetch_user(opener_id)
    if not user:
        return

    embed = discord.Embed(
        title="Ticket Closed",
        description=(
            f"Your ticket in **{guild.name}** has been closed.\n\n"
            f"Closed by: {closed_by.mention}\n"
            f"Ticket: #{channel_name}"
        ),
        color=color
    )
    embed.set_footer(text="made by @fntsheetz")

    with suppress(Exception):
        await user.send(embed=embed)


# =========================================================
# TRANSCRIPT
# =========================================================
async def fetch_channel_history_with_retry(channel: discord.TextChannel, attempts: int = 4):
    last_error = None

    for attempt in range(attempts):
        try:
            return [msg async for msg in channel.history(limit=None, oldest_first=True)]
        except (discord.DiscordServerError, discord.HTTPException) as e:
            last_error = e
            await asyncio.sleep(1.25 * (attempt + 1))
        except Exception as e:
            last_error = e
            break

    raise last_error if last_error else RuntimeError("Failed to fetch channel history.")


async def build_transcript_html(channel: discord.TextChannel) -> str:
    messages = await fetch_channel_history_with_retry(channel)

    rows = []

    for msg in messages:
        created = format_ts(msg.created_at)
        author_name = html.escape(str(msg.author))
        author_id = msg.author.id

        content = html.escape(msg.content) if msg.content else ""
        content_html = content.replace("\n", "<br>") if content else '<span class="muted">No text content</span>'

        attachment_html = ""
        if msg.attachments:
            attachment_links = []
            for att in msg.attachments:
                att_name = html.escape(att.filename)
                att_url = html.escape(att.url)
                attachment_links.append(f'<li><a href="{att_url}" target="_blank">{att_name}</a></li>')
            attachment_html = f"""
                <div class="attachments">
                    <strong>Attachments</strong>
                    <ul>{"".join(attachment_links)}</ul>
                </div>
            """

        embed_html = ""
        if msg.embeds:
            embed_parts = []
            for emb in msg.embeds:
                title_part = html.escape(emb.title) if emb.title else ""
                desc_part = html.escape(emb.description) if emb.description else ""

                inner = ""
                if title_part:
                    inner += f'<div><strong>{title_part}</strong></div>'
                if desc_part:
                    inner += f'<div class="embed-desc">{desc_part.replace(chr(10), "<br>")}</div>'

                if inner:
                    embed_parts.append(f'<div class="embed-box">{inner}</div>')

            if embed_parts:
                embed_html = f"""
                    <div class="embeds">
                        <strong>Embeds</strong>
                        {''.join(embed_parts)}
                    </div>
                """

        rows.append(f"""
            <div class="message">
                <div class="meta">
                    <span class="author">{author_name}</span>
                    <span class="author-id">({author_id})</span>
                    <span class="time">{created}</span>
                </div>
                <div class="content">{content_html}</div>
                {attachment_html}
                {embed_html}
            </div>
        """)

    guild_name = html.escape(channel.guild.name)
    channel_name = html.escape(channel.name)

    doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Transcript - #{channel_name}</title>
<style>
    body {{
        background: #0f1115;
        color: #e7eaf0;
        font-family: Arial, Helvetica, sans-serif;
        margin: 0;
        padding: 24px;
    }}
    .header {{
        margin-bottom: 24px;
        padding: 20px;
        background: #171a21;
        border: 1px solid #2a2f3a;
        border-radius: 12px;
    }}
    .header h1 {{
        margin: 0 0 8px 0;
        font-size: 24px;
    }}
    .header .sub {{
        color: #b7c0cf;
        font-size: 14px;
    }}
    .message {{
        background: #171a21;
        border: 1px solid #2a2f3a;
        border-radius: 12px;
        padding: 16px;
        margin-bottom: 14px;
    }}
    .meta {{
        margin-bottom: 10px;
        font-size: 13px;
        color: #b7c0cf;
    }}
    .author {{
        color: #ffffff;
        font-weight: bold;
    }}
    .author-id {{
        margin-left: 6px;
    }}
    .time {{
        margin-left: 12px;
    }}
    .content {{
        font-size: 15px;
        line-height: 1.5;
        margin-bottom: 10px;
        word-wrap: break-word;
    }}
    .attachments, .embeds {{
        margin-top: 10px;
        padding: 10px 12px;
        background: #101319;
        border-radius: 10px;
        border: 1px solid #252b36;
    }}
    .embed-box {{
        margin-top: 8px;
        padding: 10px;
        border-left: 4px solid #8b5cf6;
        background: #141922;
        border-radius: 8px;
    }}
    .embed-desc {{
        margin-top: 6px;
        color: #d7dce6;
    }}
    .muted {{
        color: #98a2b3;
        font-style: italic;
    }}
    a {{
        color: #8ab4ff;
        text-decoration: none;
    }}
    a:hover {{
        text-decoration: underline;
    }}
</style>
</head>
<body>
    <div class="header">
        <h1>Transcript for #{channel_name}</h1>
        <div class="sub">Guild: {guild_name} ({channel.guild.id})</div>
        <div class="sub">Channel ID: {channel.id}</div>
        <div class="sub">Generated: {format_ts(datetime.now(timezone.utc))}</div>
    </div>
    {''.join(rows) if rows else '<div class="message"><div class="content muted">No messages found.</div></div>'}
</body>
</html>
"""
    return doc


async def build_transcript_zip(channel: discord.TextChannel) -> discord.File:
    try:
        transcript_html = await build_transcript_html(channel)
    except Exception as e:
        transcript_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Transcript Error</title></head>
<body style="background:#111;color:#fff;font-family:Arial;padding:20px;">
<h1>Transcript could not be fully generated</h1>
<p>Channel: #{html.escape(channel.name)}</p>
<p>Guild: {html.escape(channel.guild.name)}</p>
<p>Error: {html.escape(str(e))}</p>
<p>Generated: {format_ts(datetime.now(timezone.utc))}</p>
</body>
</html>"""

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"transcript-{channel.name}.html", transcript_html)
    zip_buffer.seek(0)

    return discord.File(zip_buffer, filename=f"transcript-{channel.name}.zip")


# =========================================================
# GUILD PROFILE HELPERS
# =========================================================
async def patch_guild_profile_with_retry(guild_id: int, payload: dict, max_attempts: int = 5) -> tuple[bool, str]:
    headers = {
        "Authorization": f"Bot {TOKEN}",
        "Content-Type": "application/json"
    }
    url = f"https://discord.com/api/v10/guilds/{guild_id}/members/@me"

    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT) as session:
        for attempt in range(max_attempts):
            try:
                async with session.patch(url, json=payload, headers=headers) as resp:
                    if 200 <= resp.status < 300:
                        return True, "Guild profile updated successfully."

                    if resp.status == 429:
                        try:
                            data = await resp.json()
                            retry_after = float(data.get("retry_after", 1.5))
                        except Exception:
                            retry_after = 1.5
                        await asyncio.sleep(retry_after + 0.25)
                        continue

                    text = await resp.text()
                    return False, f"Discord API error {resp.status}: {text}"
            except aiohttp.ClientError:
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1.0 + attempt)
                    continue
                return False, "Discord API connection error."

    return False, "Discord API rate-limited too many times. Try again."


async def set_guild_profile(
    guild_id: int,
    nickname: Optional[str] = None,
    avatar_image: Optional[ImageData] = None,
    banner_image: Optional[ImageData] = None
) -> tuple[bool, str]:
    payload = {}

    if nickname is not None:
        payload["nick"] = nickname

    if avatar_image is not None:
        b64 = base64.b64encode(avatar_image.raw).decode("utf-8")
        payload["avatar"] = f"data:{avatar_image.mime_type};base64,{b64}"

    if banner_image is not None:
        b64 = base64.b64encode(banner_image.raw).decode("utf-8")
        payload["banner"] = f"data:{banner_image.mime_type};base64,{b64}"

    if not payload:
        return False, "Nothing to update."

    return await patch_guild_profile_with_retry(guild_id, payload)


# =========================================================
# WAITERS / QUESTION HELPERS
# =========================================================
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
    embed_builder,
    title: str,
    description: str,
    *,
    optional: bool = False,
    delete_reply: bool = True,
) -> Optional[str]:
    prompt = await channel.send(embed=embed_builder(title, description))
    reply: Optional[discord.Message] = None

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
        await safe_delete_message(prompt)
        if delete_reply and reply is not None:
            await safe_delete_message(reply)


async def ask_image(
    channel: discord.TextChannel,
    user: discord.Member,
    embed_builder,
    title: str,
    description: str,
) -> ImageData:
    while True:
        prompt = await channel.send(embed=embed_builder(title, description))
        reply: Optional[discord.Message] = None

        try:
            reply = await wait_for_user_message(channel, user)

            if reply.content.strip().lower() in CANCEL_WORDS:
                raise SetupCancelled()

            if not reply.attachments:
                await channel.send(
                    embed=embed_builder("No Image Found", "You need to upload an image in your reply."),
                    delete_after=8
                )
                continue

            attachment = reply.attachments[0]
            if not is_image_attachment(attachment):
                await channel.send(
                    embed=embed_builder("Invalid Image", "Attachment must be an image."),
                    delete_after=8
                )
                continue

            raw = await attachment.read()
            return ImageData(
                filename=attachment.filename,
                mime_type=attachment.content_type or "image/png",
                raw=raw
            )
        finally:
            await safe_delete_message(prompt)
            if reply is not None:
                await safe_delete_message(reply)


async def ask_channel_select(
    channel: discord.TextChannel,
    user: discord.Member,
    data: SetupData,
    title: str,
    description: str,
) -> discord.TextChannel:
    view = discord.ui.View(timeout=300)
    select = discord.ui.ChannelSelect(
        placeholder="Choose a text channel",
        min_values=1,
        max_values=1,
        channel_types=[discord.ChannelType.text]
    )
    view.add_item(select)

    prompt = await channel.send(embed=setup_embed(data, title, description), view=view)

    try:
        while True:
            interaction = await bot.wait_for(
                "interaction",
                check=lambda i: (
                    i.type == discord.InteractionType.component
                    and i.user.id == user.id
                    and i.channel_id == channel.id
                    and i.message is not None
                    and i.message.id == prompt.id
                ),
                timeout=300
            )

            values = (interaction.data or {}).get("values", [])
            if not values:
                await interaction.response.send_message(
                    embed=setup_embed(data, "Invalid Selection", "Please choose a text channel."),
                    ephemeral=True
                )
                continue

            channel_id = int(values[0])
            selected_channel = channel.guild.get_channel(channel_id)

            if not isinstance(selected_channel, discord.TextChannel):
                await interaction.response.send_message(
                    embed=setup_embed(data, "Invalid Selection", "That is not a valid text channel."),
                    ephemeral=True
                )
                continue

            await interaction.response.edit_message(view=None)
            return selected_channel
    finally:
        with suppress(Exception):
            await prompt.edit(view=None)


async def ask_role_select(
    channel: discord.TextChannel,
    user: discord.Member,
    data: SetupData,
    title: str,
    description: str,
) -> discord.Role:
    view = discord.ui.View(timeout=300)
    select = discord.ui.RoleSelect(
        placeholder="Choose a support role",
        min_values=1,
        max_values=1
    )
    view.add_item(select)

    prompt = await channel.send(embed=setup_embed(data, title, description), view=view)

    try:
        while True:
            interaction = await bot.wait_for(
                "interaction",
                check=lambda i: (
                    i.type == discord.InteractionType.component
                    and i.user.id == user.id
                    and i.channel_id == channel.id
                    and i.message is not None
                    and i.message.id == prompt.id
                ),
                timeout=300
            )

            values = (interaction.data or {}).get("values", [])
            if not values:
                await interaction.response.send_message(
                    embed=setup_embed(data, "Invalid Selection", "Please choose a role."),
                    ephemeral=True
                )
                continue

            role_id = int(values[0])
            selected_role = channel.guild.get_role(role_id)

            if not isinstance(selected_role, discord.Role):
                await interaction.response.send_message(
                    embed=setup_embed(data, "Invalid Selection", "That is not a valid role."),
                    ephemeral=True
                )
                continue

            await interaction.response.edit_message(view=None)
            return selected_role
    finally:
        with suppress(Exception):
            await prompt.edit(view=None)


async def ask_category_select(
    channel: discord.TextChannel,
    user: discord.Member,
    data: SetupData,
    title: str,
    description: str,
) -> discord.CategoryChannel:
    view = discord.ui.View(timeout=300)
    select = discord.ui.ChannelSelect(
        placeholder="Choose a category",
        min_values=1,
        max_values=1,
        channel_types=[discord.ChannelType.category]
    )
    view.add_item(select)

    prompt = await channel.send(embed=setup_embed(data, title, description), view=view)

    try:
        while True:
            interaction = await bot.wait_for(
                "interaction",
                check=lambda i: (
                    i.type == discord.InteractionType.component
                    and i.user.id == user.id
                    and i.channel_id == channel.id
                    and i.message is not None
                    and i.message.id == prompt.id
                ),
                timeout=300
            )

            values = (interaction.data or {}).get("values", [])
            if not values:
                await interaction.response.send_message(
                    embed=setup_embed(data, "Invalid Selection", "Please choose a category."),
                    ephemeral=True
                )
                continue

            category_id = int(values[0])
            category = channel.guild.get_channel(category_id)

            if not isinstance(category, discord.CategoryChannel):
                await interaction.response.send_message(
                    embed=setup_embed(data, "Invalid Selection", "That is not a valid category."),
                    ephemeral=True
                )
                continue

            await interaction.response.edit_message(view=None)
            return category
    finally:
        with suppress(Exception):
            await prompt.edit(view=None)


# =========================================================
# SETUP FLOW
# =========================================================
async def run_setup_wizard(interaction: discord.Interaction):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return

    guild = interaction.guild
    user = interaction.user
    channel = interaction.channel

    if not isinstance(channel, discord.TextChannel):
        await interaction.followup.send(
            embed=await base_embed(guild.id, "Setup Failed", "Setup must be run in a text channel.", error=True),
            ephemeral=True
        )
        return

    data = SetupData(guild.id, user.id, channel.id)
    setup_sessions[(guild.id, user.id)] = data
    embed_builder = lambda t, d: setup_embed(data, t, d)

    try:
        await channel.send(
            embed=setup_embed(
                data,
                "Ticket Setup Started",
                "I will ask you one question at a time.\n\n"
                "Reply in this channel.\n"
                "Type `cancel` anytime to stop the setup.\n"
                "Type `skip` on optional questions."
            )
        )

        data.title = await ask_text(channel, user, embed_builder, "Title", "Send the ticket panel title.")
        data.description = await ask_text(channel, user, embed_builder, "Description", "Send the ticket panel description.")

        color_raw = await ask_text(
            channel, user, embed_builder,
            "Embed Color",
            "Send the embed hex color.\nExample: `#00FF66`\n\nType `skip` to use the default color.",
            optional=True
        )
        if color_raw:
            try:
                data.color_hex = normalize_hex(color_raw)
            except ValueError:
                await channel.send(
                    embed=await base_embed(None, "Invalid Color", "Invalid hex color. Default color `#00FF66` will be used.", error=True),
                    delete_after=8
                )
                data.color_hex = "#00FF66"

        panel_channel = await ask_channel_select(
            channel, user, data,
            "Panel Channel",
            "Choose the channel where the ticket panel should be posted."
        )
        data.panel_channel_id = panel_channel.id

        support_role = await ask_role_select(
            channel, user, data,
            "Support Team Role",
            "Choose the support team role."
        )
        data.support_role_id = support_role.id

        data.option_1_name = await ask_text(
            channel, user, embed_builder,
            "Ticket Option 1 Name",
            "Send the name for the first ticket option.\nExample: `Support Ticket`"
        )

        option_1_category = await ask_category_select(
            channel, user, data,
            "Ticket Option 1 Category",
            "Choose the category for ticket option 1."
        )
        data.option_1_category_id = option_1_category.id

        data.option_2_name = await ask_text(
            channel, user, embed_builder,
            "Ticket Option 2 Name",
            "Send the name for the second ticket option, or type `skip`.",
            optional=True
        )

        if data.option_2_name:
            option_2_category = await ask_category_select(
                channel, user, data,
                "Ticket Option 2 Category",
                "Choose the category for ticket option 2."
            )
            data.option_2_category_id = option_2_category.id

        data.option_3_name = await ask_text(
            channel, user, embed_builder,
            "Ticket Option 3 Name",
            "Send the name for the third ticket option, or type `skip`.",
            optional=True
        )

        if data.option_3_name:
            option_3_category = await ask_category_select(
                channel, user, data,
                "Ticket Option 3 Category",
                "Choose the category for ticket option 3."
            )
            data.option_3_category_id = option_3_category.id

        log_channel = await ask_channel_select(
            channel, user, data,
            "Log Channel",
            "Choose the log channel."
        )
        data.log_channel_id = log_channel.id

        banner_attachment = await ask_image(
            channel, user, embed_builder,
            "Banner",
            "Reply with the banner image uploaded as an attachment."
        )
        data.banner_url = None

        thumbnail_attachment = await ask_image(
            channel, user, embed_builder,
            "Server PFP / Small Picture",
            "Reply with the small picture image uploaded as an attachment."
        )
        data.thumbnail_url = None

        # Re-upload by sending temp message so URLs are stable
        tmp_files = [
            discord.File(io.BytesIO(banner_attachment.raw), filename=banner_attachment.filename),
            discord.File(io.BytesIO(thumbnail_attachment.raw), filename=thumbnail_attachment.filename),
        ]
        temp_msg = await channel.send(files=tmp_files)

        if len(temp_msg.attachments) >= 2:
            data.banner_url = temp_msg.attachments[0].url
            data.thumbnail_url = temp_msg.attachments[1].url

        await channel.send(
            embed=build_setup_preview_embed(guild, data),
            view=SetupConfirmView(data)
        )

        await channel.send(
            embed=setup_embed(
                data,
                "Setup Ready",
                "Review the preview above and click **Publish** or **Cancel**."
            ),
            delete_after=20
        )

    except SetupCancelled:
        await channel.send(embed=setup_embed(data, "Setup Cancelled", "The ticket setup was cancelled."))
        cleanup_setup(guild.id, user.id)

    except Exception as e:
        log.exception("Setup wizard failed in guild %s", guild.id)
        await channel.send(
            embed=await base_embed(guild.id, "Setup Failed", f"An error happened during setup:\n`{e}`", error=True)
        )
        cleanup_setup(guild.id, user.id)


# =========================================================
# SETUP CONFIRM VIEW
# =========================================================
class SetupConfirmView(discord.ui.View):
    def __init__(self, data: SetupData):
        super().__init__(timeout=900)
        self.data = data

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.data.user_id:
            await interaction.response.send_message(
                embed=await base_embed(
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

        await ensure_interaction_ack(interaction, ephemeral=True)

        data = self.data
        guild = interaction.guild

        try:
            await clear_ticket_options(guild.id)
            await save_ticket_option(guild.id, 1, data.option_1_name, data.option_1_category_id)

            if data.option_2_name and data.option_2_category_id:
                await save_ticket_option(guild.id, 2, data.option_2_name, data.option_2_category_id)

            if data.option_3_name and data.option_3_category_id:
                await save_ticket_option(guild.id, 3, data.option_3_name, data.option_3_category_id)

            panel_channel = guild.get_channel(data.panel_channel_id)
            if not isinstance(panel_channel, discord.TextChannel):
                await safe_component_reply(
                    interaction,
                    embed=await base_embed(guild.id, "Publish Failed", "Panel channel is invalid.", error=True),
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title=data.title,
                description=data.description,
                color=hex_to_color(data.color_hex)
            )
            if data.thumbnail_url:
                embed.set_thumbnail(url=data.thumbnail_url)
            if data.banner_url:
                embed.set_image(url=data.banner_url)
            embed.set_footer(text="made by @fntsheetz", icon_url=get_bot_guild_avatar_url(guild.id))

            panel_view = await TicketPanelView.build(guild.id)
            panel_message = await panel_channel.send(embed=embed, view=panel_view)

            await save_guild_config(
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

            bot.add_view(await TicketPanelView.build(guild.id), message_id=panel_message.id)

            with suppress(Exception):
                if interaction.message:
                    await interaction.message.edit(
                        embed=await base_embed(guild.id, "Setup Complete", f"Ticket panel created in {panel_channel.mention}."),
                        view=None
                    )

            await safe_component_reply(
                interaction,
                embed=await base_embed(guild.id, "Setup Complete", f"Ticket panel created in {panel_channel.mention}."),
                ephemeral=True
            )

            cleanup_setup(guild.id, interaction.user.id)

        except Exception as e:
            log.exception("Failed to publish setup in guild %s", guild.id)
            await safe_component_reply(
                interaction,
                embed=await base_embed(guild.id, "Publish Failed", f"An error happened:\n`{e}`", error=True),
                ephemeral=True
            )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await ensure_interaction_ack(interaction, ephemeral=True)

        with suppress(Exception):
            if interaction.message:
                await interaction.message.edit(
                    embed=await base_embed(
                        interaction.guild.id if interaction.guild else None,
                        "Setup Cancelled",
                        "The setup was cancelled."
                    ),
                    view=None
                )

        if interaction.guild:
            cleanup_setup(interaction.guild.id, interaction.user.id)

        await safe_component_reply(
            interaction,
            embed=await base_embed(
                interaction.guild.id if interaction.guild else None,
                "Setup Cancelled",
                "The setup was cancelled."
            ),
            ephemeral=True
        )


# =========================================================
# PANEL VIEW
# =========================================================
class TicketDropdown(discord.ui.Select):
    def __init__(self, guild_id: int, rows: list[dict[str, Any]]):
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
        self.guild_id = guild_id

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        guild = interaction.guild
        opener = interaction.user

        await ensure_interaction_ack(interaction, ephemeral=True)

        config = await get_guild_config(guild.id)
        if not config:
            await safe_component_reply(
                interaction,
                embed=await base_embed(guild.id, "Error", "Ticket system is not configured.", error=True),
                ephemeral=True
            )
            return

        await refresh_panel_message(interaction.message, guild.id)

        create_lock = get_ticket_create_lock(guild.id, opener.id)
        async with create_lock:
            try:
                existing = await get_open_ticket_for_user(guild.id, opener.id)
                if existing:
                    existing_channel = guild.get_channel(existing["channel_id"])
                    if existing_channel:
                        embed = await base_embed(
                            guild.id,
                            "Open Ticket Found",
                            f"You already have an open ticket: {existing_channel.mention}"
                        )
                        ok = await safe_edit_original_response(interaction, embed=embed)
                        if not ok:
                            await safe_component_reply(interaction, embed=embed, ephemeral=True)
                        return

                try:
                    selected_index = int(self.values[0])
                except Exception:
                    embed = await base_embed(guild.id, "Error", "Invalid selection.", error=True)
                    ok = await safe_edit_original_response(interaction, embed=embed)
                    if not ok:
                        await safe_component_reply(interaction, embed=embed, ephemeral=True)
                    return

                rows = await get_ticket_options(guild.id)
                selected = next((r for r in rows if r["option_index"] == selected_index), None)

                if not selected:
                    embed = await base_embed(guild.id, "Error", "That ticket option is no longer configured.", error=True)
                    ok = await safe_edit_original_response(interaction, embed=embed)
                    if not ok:
                        await safe_component_reply(interaction, embed=embed, ephemeral=True)
                    return

                category = guild.get_channel(selected["category_id"])
                if not isinstance(category, discord.CategoryChannel):
                    embed = await base_embed(guild.id, "Error", "The configured category is invalid.", error=True)
                    ok = await safe_edit_original_response(interaction, embed=embed)
                    if not ok:
                        await safe_component_reply(interaction, embed=embed, ephemeral=True)
                    return

                support_role = guild.get_role(config["support_role_id"])
                if not support_role:
                    embed = await base_embed(guild.id, "Error", "The support role is invalid.", error=True)
                    ok = await safe_edit_original_response(interaction, embed=embed)
                    if not ok:
                        await safe_component_reply(interaction, embed=embed, ephemeral=True)
                    return

                me = guild.me
                if me is None:
                    embed = await base_embed(guild.id, "Error", "Bot member could not be resolved in this server.", error=True)
                    ok = await safe_edit_original_response(interaction, embed=embed)
                    if not ok:
                        await safe_component_reply(interaction, embed=embed, ephemeral=True)
                    return

                base_name = clean_channel_name(f"{selected['label']}-{opener.name}")
                channel_name = base_name

                existing_names = {c.name for c in guild.channels}
                counter = 2
                while channel_name in existing_names:
                    suffix = f"-{counter}"
                    channel_name = f"{base_name[:80-len(suffix)]}{suffix}"
                    counter += 1

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
                    me: discord.PermissionOverwrite(
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
                    embed = await base_embed(
                        guild.id,
                        "Error",
                        "I do not have permission to create channels in that category.",
                        error=True
                    )
                    ok = await safe_edit_original_response(interaction, embed=embed)
                    if not ok:
                        await safe_component_reply(interaction, embed=embed, ephemeral=True)
                    return
                except discord.HTTPException as e:
                    embed = await base_embed(
                        guild.id,
                        "Error",
                        f"Failed to create the ticket channel.\n`{e}`",
                        error=True
                    )
                    ok = await safe_edit_original_response(interaction, embed=embed)
                    if not ok:
                        await safe_component_reply(interaction, embed=embed, ephemeral=True)
                    return

                await create_ticket_record(ticket_channel.id, guild.id, opener.id, selected["label"])

                ticket_view = TicketControlsView()
                embed = await build_ticket_embed(guild.id, selected["label"], opener)

                await ticket_channel.send(
                    content=f"{support_role.mention} {opener.mention}",
                    embed=embed,
                    view=ticket_view
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

                result_embed = await base_embed(guild.id, "Ticket Created", f"Your ticket has been created: {ticket_channel.mention}")
                ok = await safe_edit_original_response(interaction, embed=result_embed)
                if not ok:
                    await safe_component_reply(interaction, embed=result_embed, ephemeral=True)

            except Exception as e:
                log.exception("Ticket creation failed in guild %s for user %s", guild.id, opener.id)
                embed = await base_embed(guild.id, "Error", f"Failed to create ticket.\n`{e}`", error=True)
                ok = await safe_edit_original_response(interaction, embed=embed)
                if not ok:
                    await safe_component_reply(interaction, embed=embed, ephemeral=True)


class TicketPanelView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        self.guild_id = guild_id

    @classmethod
    async def build(cls, guild_id: int) -> "TicketPanelView":
        self = cls(guild_id)
        rows = await get_ticket_options(guild_id)
        if rows:
            self.add_item(TicketDropdown(guild_id, rows))
        return self


# =========================================================
# TICKET BUTTONS
# =========================================================
class ClaimTicketButton(discord.ui.Button):
    def __init__(self, disabled: bool = False, *, closed_variant: bool = False):
        super().__init__(
            label="Claim Ticket",
            style=discord.ButtonStyle.secondary,
            custom_id="ticket_claim_button_closed" if closed_variant else "ticket_claim_button",
            disabled=disabled
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            return

        await ensure_interaction_ack(interaction, ephemeral=True)

        lock = get_ticket_channel_lock(interaction.channel.id)
        async with lock:
            ticket = await get_ticket_by_channel(interaction.channel.id)
            if not ticket:
                await safe_component_reply(
                    interaction,
                    embed=await base_embed(interaction.guild.id, "Error", "This is not a tracked ticket channel.", error=True),
                    ephemeral=True
                )
                return

            if ticket["status"] == "closed":
                await safe_component_reply(
                    interaction,
                    embed=await base_embed(interaction.guild.id, "Ticket Closed", "This ticket is already closed."),
                    ephemeral=True
                )
                return

            if not await is_support_or_admin(interaction.user, interaction.guild.id):
                await safe_component_reply(
                    interaction,
                    embed=await base_embed(interaction.guild.id, "Access Denied", "Only the support team or admins can claim tickets.", error=True),
                    ephemeral=True
                )
                return

            if ticket["claimed_by"] == interaction.user.id:
                await safe_component_reply(
                    interaction,
                    embed=await base_embed(interaction.guild.id, "Already Claimed", "You already claimed this ticket."),
                    ephemeral=True
                )
                return

            await set_ticket_claimed(interaction.channel.id, interaction.user.id)

            await safe_component_reply(
                interaction,
                embed=await base_embed(interaction.guild.id, "Ticket Claimed", f"Ticket claimed by {interaction.user.mention}."),
                ephemeral=False
            )

            await send_log(
                interaction.guild,
                "Ticket Claimed",
                f"Channel: {interaction.channel.mention}\nClaimed by: {interaction.user.mention}"
            )


class CloseTicketButton(discord.ui.Button):
    def __init__(self, disabled: bool = False, *, closed_variant: bool = False):
        super().__init__(
            label="Close Ticket",
            style=discord.ButtonStyle.danger,
            custom_id="ticket_close_button_closed" if closed_variant else "ticket_close_button",
            disabled=disabled
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            return

        await ensure_interaction_ack(interaction, ephemeral=True)

        lock = get_ticket_channel_lock(interaction.channel.id)
        async with lock:
            ticket = await get_ticket_by_channel(interaction.channel.id)
            if not ticket:
                await safe_component_reply(
                    interaction,
                    embed=await base_embed(interaction.guild.id, "Error", "This is not a tracked ticket channel.", error=True),
                    ephemeral=True
                )
                return

            if ticket["status"] == "closed":
                await safe_component_reply(
                    interaction,
                    embed=await base_embed(interaction.guild.id, "Already Closed", "This ticket is already closed."),
                    ephemeral=True
                )
                return

            if not await is_support_or_admin(interaction.user, interaction.guild.id):
                await safe_component_reply(
                    interaction,
                    embed=await base_embed(interaction.guild.id, "Access Denied", "Only the support team or admins can close tickets.", error=True),
                    ephemeral=True
                )
                return

            config = await get_guild_config(interaction.guild.id)
            support_role = interaction.guild.get_role(config["support_role_id"]) if config else None
            opener_member = await try_fetch_member(interaction.guild, ticket["opener_id"])

            try:
                if opener_member:
                    await interaction.channel.set_permissions(opener_member, view_channel=False)
            except discord.Forbidden:
                await safe_component_reply(
                    interaction,
                    embed=await base_embed(interaction.guild.id, "Error", "I do not have permission to update channel permissions.", error=True),
                    ephemeral=True
                )
                return

            if support_role:
                try:
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
                    await safe_component_reply(
                        interaction,
                        embed=await base_embed(interaction.guild.id, "Error", "I do not have permission to update support role permissions.", error=True),
                        ephemeral=True
                    )
                    return

            await close_ticket_record(interaction.channel.id, interaction.user.id)

            try:
                if interaction.message:
                    await interaction.message.edit(view=ClosedTicketControlsView())
            except Exception:
                pass

            await interaction.channel.send(embed=await build_closed_ticket_embed(interaction.guild.id, interaction.user))

            await dm_ticket_closed(
                interaction.guild,
                ticket["opener_id"],
                interaction.user,
                interaction.channel.name
            )

            await send_log(
                interaction.guild,
                "Ticket Closed",
                f"Channel: #{interaction.channel.name}\nClosed by: {interaction.user.mention}"
            )

            await safe_component_reply(
                interaction,
                embed=await base_embed(interaction.guild.id, "Ticket Closed", f"Ticket closed by {interaction.user.mention}."),
                ephemeral=True
            )


class DeleteTicketButton(discord.ui.Button):
    def __init__(self, *, closed_variant: bool = False):
        super().__init__(
            label="Delete Ticket",
            style=discord.ButtonStyle.danger,
            custom_id="ticket_delete_button_closed" if closed_variant else "ticket_delete_button"
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            return

        await ensure_interaction_ack(interaction, ephemeral=False)

        lock = get_ticket_channel_lock(interaction.channel.id)
        async with lock:
            ticket = await get_ticket_by_channel(interaction.channel.id)
            if not ticket:
                await safe_component_reply(
                    interaction,
                    embed=await base_embed(interaction.guild.id, "Error", "This is not a tracked ticket channel.", error=True),
                    ephemeral=True
                )
                return

            if not await is_support_or_admin(interaction.user, interaction.guild.id):
                await safe_component_reply(
                    interaction,
                    embed=await base_embed(interaction.guild.id, "Access Denied", "Only the support team or admins can delete tickets.", error=True),
                    ephemeral=True
                )
                return

            transcript_file = await build_transcript_zip(interaction.channel)
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

            await delete_ticket_record(interaction.channel.id)

            with suppress(Exception):
                await interaction.followup.send(
                    embed=await base_embed(interaction.guild.id, "Deleting Ticket", "The ticket is being deleted.")
                )

            with suppress(Exception):
                await interaction.channel.delete(reason=f"Ticket deleted by {interaction.user}")


class TicketControlsView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(ClaimTicketButton())
        self.add_item(CloseTicketButton())
        self.add_item(DeleteTicketButton())


class ClosedTicketControlsView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(ClaimTicketButton(disabled=True, closed_variant=True))
        self.add_item(CloseTicketButton(disabled=True, closed_variant=True))
        self.add_item(DeleteTicketButton(closed_variant=True))


# =========================================================
# COMMANDS
# =========================================================
@bot.tree.command(name="setup", description="Start the guided ticket setup")
@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
async def setup(interaction: discord.Interaction):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return

    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            embed=await base_embed(None, "Access Denied", "Only users with Administrator can use this command.", error=True),
            ephemeral=True
        )
        return

    if interaction.guild.id in active_setup_guilds:
        await interaction.response.send_message(
            embed=await base_embed(interaction.guild.id, "Setup Running", "A setup is already running in this server."),
            ephemeral=True
        )
        return

    active_setup_guilds.add(interaction.guild.id)

    await interaction.response.send_message(
        embed=await base_embed(interaction.guild.id, "Setup Started", "The guided setup has started in this channel."),
        ephemeral=True
    )

    await run_setup_wizard(interaction)


@bot.tree.command(name="setprofile", description="Change the bot nickname, avatar and banner for this server")
@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
async def setprofile(
    interaction: discord.Interaction,
    nickname: Optional[str] = None,
    avatar: Optional[discord.Attachment] = None,
    banner: Optional[discord.Attachment] = None,
):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return

    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(
            embed=await base_embed(None, "Access Denied", "Only users with Administrator can use this command.", error=True),
            ephemeral=True
        )
        return

    if avatar is not None and not is_image_attachment(avatar):
        await interaction.response.send_message(
            embed=await base_embed(None, "Update Failed", "Avatar attachment must be an image.", error=True),
            ephemeral=True
        )
        return

    if banner is not None and not is_image_attachment(banner):
        await interaction.response.send_message(
            embed=await base_embed(None, "Update Failed", "Banner attachment must be an image.", error=True),
            ephemeral=True
        )
        return

    if nickname is None and avatar is None and banner is None:
        await interaction.response.send_message(
            embed=await base_embed(interaction.guild.id, "Update Failed", "You need to provide at least one value to update.", error=True),
            ephemeral=True
        )
        return

    await safe_defer(interaction, ephemeral=True, thinking=True)

    avatar_image = None
    banner_image = None

    try:
        if avatar is not None:
            avatar_raw = await avatar.read()
            avatar_image = ImageData(
                filename=avatar.filename,
                mime_type=avatar.content_type or "image/png",
                raw=avatar_raw
            )

        if banner is not None:
            banner_raw = await banner.read()
            banner_image = ImageData(
                filename=banner.filename,
                mime_type=banner.content_type or "image/png",
                raw=banner_raw
            )

        success, message = await set_guild_profile(
            guild_id=interaction.guild.id,
            nickname=nickname,
            avatar_image=avatar_image,
            banner_image=banner_image
        )

        if success:
            await interaction.followup.send(
                embed=await base_embed(
                    interaction.guild.id,
                    "Server Profile Updated",
                    "The bot profile was updated for this server."
                ),
                ephemeral=True
            )
        else:
            await interaction.followup.send(
                embed=await base_embed(interaction.guild.id, "Update Failed", message, error=True),
                ephemeral=True
            )

    except Exception as e:
        log.exception("Setprofile command failed in guild %s", interaction.guild.id)
        await interaction.followup.send(
            embed=await base_embed(interaction.guild.id, "Update Failed", f"An error happened:\n`{e}`", error=True),
            ephemeral=True
        )


@bot.tree.command(name="remind", description="DM a user to reply in their ticket")
@app_commands.guild_only()
async def remind(interaction: discord.Interaction, user: discord.Member, message: str):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return

    if not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message(
            embed=await base_embed(interaction.guild.id, "Error", "This command must be used inside a ticket channel.", error=True),
            ephemeral=True
        )
        return

    if not await is_support_or_admin(interaction.user, interaction.guild.id):
        await interaction.response.send_message(
            embed=await base_embed(interaction.guild.id, "Access Denied", "Only the support team or admins can use this command.", error=True),
            ephemeral=True
        )
        return

    ticket = await get_ticket_by_channel(interaction.channel.id)
    if not ticket:
        await interaction.response.send_message(
            embed=await base_embed(interaction.guild.id, "Error", "This command can only be used inside a ticket channel.", error=True),
            ephemeral=True
        )
        return

    if ticket["opener_id"] != user.id:
        await interaction.response.send_message(
            embed=await base_embed(interaction.guild.id, "Error", "That user is not the opener of this ticket.", error=True),
            ephemeral=True
        )
        return

    config = await get_guild_config(interaction.guild.id)
    dm_embed = discord.Embed(
        title="Ticket Reminder",
        description=(
            f"You have an open ticket in **{interaction.guild.name}**.\n\n"
            f"Message from support:\n{message}\n\n"
            f"Ticket channel: #{interaction.channel.name}"
        ),
        color=hex_to_color(config["color_hex"]) if config else discord.Color.green()
    )
    dm_embed.set_footer(text="made by @fntsheetz")

    try:
        await user.send(embed=dm_embed)
    except discord.Forbidden:
        await interaction.response.send_message(
            embed=await base_embed(interaction.guild.id, "DM Failed", "I could not DM that user.", error=True),
            ephemeral=True
        )
        return

    await interaction.response.send_message(
        embed=await base_embed(interaction.guild.id, "Reminder Sent", f"Reminder sent to {user.mention}."),
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


# =========================================================
# COMMAND ERROR HANDLERS
# =========================================================
@setup.error
async def setup_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if interaction.guild:
        cleanup_setup(interaction.guild.id, interaction.user.id)

    embed = await base_embed(
        interaction.guild.id if interaction.guild else None,
        "Setup Failed",
        f"{error}",
        error=True
    )

    try:
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)
    except Exception:
        pass


@setprofile.error
async def setprofile_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    embed = await base_embed(
        interaction.guild.id if interaction.guild else None,
        "Update Failed",
        f"{error}",
        error=True
    )

    try:
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)
    except Exception:
        pass


@remind.error
async def remind_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    embed = await base_embed(
        interaction.guild.id if interaction.guild else None,
        "Remind Failed",
        f"{error}",
        error=True
    )

    try:
        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, ephemeral=True)
    except Exception:
        pass


# =========================================================
# VIEW RESTORATION
# =========================================================
async def restore_persistent_views():
    bot.add_view(TicketControlsView())
    bot.add_view(ClosedTicketControlsView())

    rows = await get_all_panel_rows()
    for row in rows:
        try:
            view = await TicketPanelView.build(row["guild_id"])
            bot.add_view(view, message_id=row["panel_message_id"])
        except Exception as e:
            log.warning("Failed to restore panel view for guild %s: %s", row["guild_id"], e)


# =========================================================
# EVENTS
# =========================================================
@bot.event
async def on_ready():
    await init_db()

    try:
        synced = await bot.tree.sync()
        log.info("Synced %s commands", len(synced))
    except Exception as e:
        log.warning("Command sync failed: %s", e)

    await restore_persistent_views()

    if bot.user:
        log.info("Logged in as %s (%s)", bot.user, bot.user.id)


@bot.event
async def on_guild_remove(guild: discord.Guild):
    guild_config_cache.pop(guild.id, None)
    ticket_options_cache.pop(guild.id, None)
    active_setup_guilds.discard(guild.id)


@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.TextChannel):
        return

    try:
        ticket = await get_ticket_by_channel(channel.id)
        if ticket:
            await delete_ticket_record(channel.id)
            ticket_channel_locks.pop(channel.id, None)
    except Exception:
        pass


# =========================================================
# CLEAN SHUTDOWN
# =========================================================
async def close_resources():
    global db_pool
    if db_pool is not None:
        await db_pool.close()
        db_pool = None
        log.info("Postgres pool closed.")


# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    try:
        bot.run(TOKEN, log_handler=None)
    finally:
        with suppress(Exception):
            asyncio.run(close_resources())
