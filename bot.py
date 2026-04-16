import aiohttp
import asyncio
import asyncpg
import base64
import html
import io
import logging
import os
import re
import sys
import traceback
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Any

import discord
from discord import app_commands
from discord.ext import commands


# ============================================================
# CONFIG
# ============================================================
TOKEN = os.getenv("DISCORD_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN is not set.")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set. Attach PostgreSQL on Railway and expose DATABASE_URL.")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
DEFAULT_COLOR = "#00FF66"
FOOTER_TEXT = "made by @fntsheetz"
MAX_WIZARD_WAIT = 300
PROFILE_PATCH_MAX_ATTEMPTS = 5
CHANNEL_CREATE_RETRY_ATTEMPTS = 3
DELETE_DELAY_SECONDS = 1.25

SKIP_WORDS = {"skip", "none", "no", "-"}
CANCEL_WORDS = {"cancel", "stop", "abort", "exit"}

# Optional dedicated asset storage channel. If not set, panel attachments are stored on the panel message itself.
ASSET_CHANNEL_ID = int(os.getenv("ASSET_CHANNEL_ID", "0") or 0)

# If true, sync tree once per process on ready.
SYNC_COMMAND_TREE = os.getenv("SYNC_COMMAND_TREE", "true").lower() == "true"


# ============================================================
# LOGGING
# ============================================================
logger = logging.getLogger("ticketbot")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)


# ============================================================
# DISCORD SETUP
# ============================================================
intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix=commands.when_mentioned, intents=intents)


# ============================================================
# GLOBAL STATE
# ============================================================
db_pool: Optional[asyncpg.Pool] = None
command_tree_synced = False
startup_once_lock = asyncio.Lock()

# cache
config_cache: dict[int, dict[str, Any]] = {}
options_cache: dict[int, list[dict[str, Any]]] = {}

# locks
setup_guild_locks: dict[int, asyncio.Lock] = {}
ticket_channel_locks: dict[int, asyncio.Lock] = {}
ticket_create_locks: dict[tuple[int, int], asyncio.Lock] = {}
setprofile_guild_locks: dict[int, asyncio.Lock] = {}

# state
active_setup_guilds: set[int] = set()
active_setprofile_guilds: set[int] = set()
setup_sessions: dict[tuple[int, int], "SetupData"] = {}
setprofile_sessions: dict[tuple[int, int], "SetProfileData"] = {}


# ============================================================
# STATE MODELS
# ============================================================
class SetupCancelled(Exception):
    pass


@dataclass(slots=True)
class StoredAsset:
    filename: str
    data: bytes
    preview_url: Optional[str] = None
    final_url: Optional[str] = None


@dataclass(slots=True)
class SetupData:
    guild_id: int
    user_id: int
    setup_channel_id: int

    title: Optional[str] = None
    description: Optional[str] = None
    color_hex: str = DEFAULT_COLOR

    panel_channel_id: Optional[int] = None
    support_role_id: Optional[int] = None
    log_channel_id: Optional[int] = None

    option_1_name: Optional[str] = None
    option_1_category_id: Optional[int] = None
    option_2_name: Optional[str] = None
    option_2_category_id: Optional[int] = None
    option_3_name: Optional[str] = None
    option_3_category_id: Optional[int] = None

    banner: Optional[StoredAsset] = None
    thumbnail: Optional[StoredAsset] = None


@dataclass(slots=True)
class SetProfileData:
    guild_id: int
    user_id: int
    channel_id: int
    display: Optional[str] = None
    avatar_attachment: Optional[discord.Attachment] = None
    banner_attachment: Optional[discord.Attachment] = None


# ============================================================
# LOCK HELPERS
# ============================================================
def get_setup_guild_lock(guild_id: int) -> asyncio.Lock:
    lock = setup_guild_locks.get(guild_id)
    if lock is None:
        lock = asyncio.Lock()
        setup_guild_locks[guild_id] = lock
    return lock



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



def get_setprofile_guild_lock(guild_id: int) -> asyncio.Lock:
    lock = setprofile_guild_locks.get(guild_id)
    if lock is None:
        lock = asyncio.Lock()
        setprofile_guild_locks[guild_id] = lock
    return lock


# ============================================================
# SESSION CLEANUP
# ============================================================
def cleanup_setup(guild_id: int, user_id: int):
    active_setup_guilds.discard(guild_id)
    setup_sessions.pop((guild_id, user_id), None)



def cleanup_setprofile(guild_id: int, user_id: int):
    active_setprofile_guilds.discard(guild_id)
    setprofile_sessions.pop((guild_id, user_id), None)


# ============================================================
# DB
# ============================================================
async def create_db_pool():
    global db_pool
    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=12,
        command_timeout=60,
        server_settings={"application_name": "advanced_ticket_bot"},
    )
    logger.info("PostgreSQL pool created")


async def init_db():
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
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
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ticket_options (
                guild_id BIGINT NOT NULL,
                option_index INTEGER NOT NULL,
                label TEXT NOT NULL,
                category_id BIGINT NOT NULL,
                PRIMARY KEY (guild_id, option_index)
            )
            """
        )

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tickets (
                channel_id BIGINT PRIMARY KEY,
                guild_id BIGINT NOT NULL,
                opener_id BIGINT NOT NULL,
                option_label TEXT NOT NULL,
                status TEXT NOT NULL,
                claimed_by BIGINT,
                closed_by BIGINT,
                created_at TIMESTAMPTZ NOT NULL,
                closed_at TIMESTAMPTZ
            )
            """
        )

        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tickets_guild_opener_status
            ON tickets(guild_id, opener_id, status)
            """
        )

        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tickets_status
            ON tickets(status)
            """
        )

        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ticket_options_guild
            ON ticket_options(guild_id)
            """
        )
    logger.info("Database initialized")


async def load_cache():
    assert db_pool is not None
    config_cache.clear()
    options_cache.clear()

    async with db_pool.acquire() as conn:
        config_rows = await conn.fetch("SELECT * FROM guild_config")
        for row in config_rows:
            config_cache[int(row["guild_id"])] = dict(row)

        option_rows = await conn.fetch(
            "SELECT * FROM ticket_options ORDER BY guild_id ASC, option_index ASC"
        )
        for row in option_rows:
            gid = int(row["guild_id"])
            options_cache.setdefault(gid, []).append(dict(row))

    logger.info("Caches loaded: %s guild configs, %s option groups", len(config_cache), len(options_cache))


async def refresh_guild_cache(guild_id: int):
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        config = await conn.fetchrow(
            "SELECT * FROM guild_config WHERE guild_id = $1",
            guild_id,
        )
        if config:
            config_cache[guild_id] = dict(config)
        else:
            config_cache.pop(guild_id, None)

        option_rows = await conn.fetch(
            "SELECT * FROM ticket_options WHERE guild_id = $1 ORDER BY option_index ASC",
            guild_id,
        )
        options_cache[guild_id] = [dict(row) for row in option_rows]


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
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO guild_config (
                guild_id, panel_channel_id, panel_message_id, title, description,
                color_hex, banner_url, thumbnail_url, support_role_id, log_channel_id, updated_at
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
            ON CONFLICT (guild_id) DO UPDATE SET
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
            """,
            guild_id,
            panel_channel_id,
            panel_message_id,
            title,
            description,
            color_hex,
            banner_url,
            thumbnail_url,
            support_role_id,
            log_channel_id,
        )
    await refresh_guild_cache(guild_id)


async def clear_ticket_options(guild_id: int):
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM ticket_options WHERE guild_id = $1", guild_id)
    await refresh_guild_cache(guild_id)


async def save_ticket_option(guild_id: int, option_index: int, label: str, category_id: int):
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ticket_options (guild_id, option_index, label, category_id)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (guild_id, option_index) DO UPDATE SET
                label = EXCLUDED.label,
                category_id = EXCLUDED.category_id
            """,
            guild_id,
            option_index,
            label,
            category_id,
        )
    await refresh_guild_cache(guild_id)


async def create_ticket_record(channel_id: int, guild_id: int, opener_id: int, option_label: str):
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO tickets (
                channel_id, guild_id, opener_id, option_label, status,
                claimed_by, closed_by, created_at, closed_at
            )
            VALUES ($1,$2,$3,$4,'open',NULL,NULL,$5,NULL)
            """,
            channel_id,
            guild_id,
            opener_id,
            option_label,
            datetime.now(timezone.utc),
        )


async def get_ticket_by_channel(channel_id: int) -> Optional[dict[str, Any]]:
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM tickets WHERE channel_id = $1", channel_id)
    return dict(row) if row else None


async def get_open_ticket_for_user(guild_id: int, opener_id: int) -> Optional[dict[str, Any]]:
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT * FROM tickets
            WHERE guild_id = $1 AND opener_id = $2 AND status = 'open'
            LIMIT 1
            """,
            guild_id,
            opener_id,
        )
    return dict(row) if row else None


async def set_ticket_claimed(channel_id: int, claimed_by: int):
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE tickets SET claimed_by = $1 WHERE channel_id = $2",
            claimed_by,
            channel_id,
        )


async def close_ticket_record(channel_id: int, closed_by: Optional[int]):
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE tickets
            SET status = 'closed',
                closed_by = $1,
                closed_at = $2
            WHERE channel_id = $3
            """,
            closed_by,
            datetime.now(timezone.utc),
            channel_id,
        )


async def delete_ticket_record(channel_id: int):
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM tickets WHERE channel_id = $1", channel_id)


async def get_all_panel_rows() -> list[dict[str, Any]]:
    assert db_pool is not None
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT guild_id, panel_message_id FROM guild_config")
    return [dict(r) for r in rows]


# ============================================================
# CACHE HELPERS
# ============================================================
def get_guild_config(guild_id: Optional[int]) -> Optional[dict[str, Any]]:
    if guild_id is None:
        return None
    return config_cache.get(guild_id)



def get_ticket_options(guild_id: int) -> list[dict[str, Any]]:
    return options_cache.get(guild_id, [])


# ============================================================
# UTILS
# ============================================================
def normalize_hex(value: str) -> str:
    clean = value.strip().replace("#", "")
    if not re.fullmatch(r"[0-9a-fA-F]{6}", clean):
        raise ValueError("Invalid hex color")
    return f"#{clean.upper()}"



def hex_to_color(value: str) -> discord.Color:
    return discord.Color(int(value.replace("#", ""), 16))



def clean_channel_name(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^a-z0-9\- ]", "", text)
    text = re.sub(r"\s+", "-", text).strip("-")
    text = re.sub(r"-{2,}", "-", text)
    return text[:90] if text else "ticket"



def is_image_attachment(att: discord.Attachment) -> bool:
    if att.content_type and att.content_type.startswith("image/"):
        return True
    filename = att.filename.lower()
    return filename.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp"))



def format_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")



def bot_guild_avatar_url(guild_id: Optional[int]) -> Optional[str]:
    if bot.user is None:
        return None
    if guild_id is None:
        return bot.user.display_avatar.url
    guild = bot.get_guild(guild_id)
    if not guild:
        return bot.user.display_avatar.url
    me = guild.me or guild.get_member(bot.user.id)
    if me:
        return me.display_avatar.url
    return bot.user.display_avatar.url



def base_embed(guild_id: Optional[int], title: str, description: str, *, error: bool = False) -> discord.Embed:
    color = discord.Color.red() if error else hex_to_color(get_guild_config(guild_id)["color_hex"]) if get_guild_config(guild_id) else discord.Color.green()
    embed = discord.Embed(title=title, description=description, color=color)
    embed.set_footer(text=FOOTER_TEXT, icon_url=bot_guild_avatar_url(guild_id))
    return embed



def setup_embed(data: SetupData, title: str, description: str) -> discord.Embed:
    embed = discord.Embed(title=title, description=description, color=hex_to_color(data.color_hex))
    embed.set_footer(text=FOOTER_TEXT, icon_url=bot_guild_avatar_url(data.guild_id))
    return embed



def build_setup_preview_embed(guild: discord.Guild, data: SetupData) -> discord.Embed:
    embed = discord.Embed(
        title=data.title or "Setup Preview",
        description=data.description or "No description set.",
        color=hex_to_color(data.color_hex),
    )

    panel_channel = guild.get_channel(data.panel_channel_id) if data.panel_channel_id else None
    log_channel = guild.get_channel(data.log_channel_id) if data.log_channel_id else None
    support_role = guild.get_role(data.support_role_id) if data.support_role_id else None

    cat1 = guild.get_channel(data.option_1_category_id) if data.option_1_category_id else None
    cat2 = guild.get_channel(data.option_2_category_id) if data.option_2_category_id else None
    cat3 = guild.get_channel(data.option_3_category_id) if data.option_3_category_id else None

    embed.add_field(
        name="Panel Settings",
        value=(
            f"**Color:** `{data.color_hex}`\n"
            f"**Panel Channel:** {panel_channel.mention if panel_channel else '`Not set`'}\n"
            f"**Support Team:** {support_role.mention if support_role else '`Not set`'}\n"
            f"**Log Channel:** {log_channel.mention if log_channel else '`Not set`'}"
        ),
        inline=False,
    )

    embed.add_field(
        name="Ticket Options",
        value=(
            f"**1.** {data.option_1_name or '`Not set`'} - {cat1.name if cat1 else '`Not set`'}\n"
            f"**2.** {data.option_2_name or '`Skipped`'} - {(cat2.name if cat2 else ('`Skipped`' if not data.option_2_name else '`Not set`'))}\n"
            f"**3.** {data.option_3_name or '`Skipped`'} - {(cat3.name if cat3 else ('`Skipped`' if not data.option_3_name else '`Not set`'))}"
        ),
        inline=False,
    )

    if data.thumbnail and data.thumbnail.preview_url:
        embed.set_thumbnail(url=data.thumbnail.preview_url)
    if data.banner and data.banner.preview_url:
        embed.set_image(url=data.banner.preview_url)

    embed.set_footer(text=FOOTER_TEXT, icon_url=bot_guild_avatar_url(guild.id))
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
        color=color,
    )
    embed.set_footer(text=FOOTER_TEXT, icon_url=bot_guild_avatar_url(guild_id))
    return embed



def build_closed_ticket_embed(guild_id: int, closed_by: discord.Member) -> discord.Embed:
    config = get_guild_config(guild_id)
    color = hex_to_color(config["color_hex"]) if config else discord.Color.green()
    embed = discord.Embed(
        title="Ticket Closed",
        description=f"Ticket closed by {closed_by.mention}.",
        color=color,
    )
    embed.set_footer(text=FOOTER_TEXT, icon_url=bot_guild_avatar_url(guild_id))
    return embed



def is_support_or_admin(member: discord.Member, guild_id: int) -> bool:
    if member.guild_permissions.administrator:
        return True
    config = get_guild_config(guild_id)
    if not config:
        return False
    role = member.guild.get_role(config["support_role_id"])
    return role in member.roles if role else False


async def try_fetch_member(guild: discord.Guild, user_id: int) -> Optional[discord.Member]:
    member = guild.get_member(user_id)
    if member:
        return member
    try:
        return await guild.fetch_member(user_id)
    except Exception:
        return None


async def try_fetch_user(user_id: int) -> Optional[discord.User]:
    user = bot.get_user(user_id)
    if user:
        return user
    try:
        return await bot.fetch_user(user_id)
    except Exception:
        return None


async def safe_delete(message: Optional[discord.Message]):
    if not message:
        return
    try:
        await message.delete()
    except Exception:
        pass


async def safe_defer(interaction: discord.Interaction, *, ephemeral: bool = False, thinking: bool = False) -> bool:
    try:
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=ephemeral, thinking=thinking)
            return True
    except Exception:
        return False
    return False


async def safe_send_interaction(
    interaction: discord.Interaction,
    *,
    content: Optional[str] = None,
    embed: Optional[discord.Embed] = None,
    view: Optional[discord.ui.View] = None,
    ephemeral: bool = False,
):
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message(content=content, embed=embed, view=view, ephemeral=ephemeral)
            return True
    except Exception:
        pass

    try:
        await interaction.followup.send(content=content, embed=embed, view=view, ephemeral=ephemeral)
        return True
    except Exception:
        return False


async def safe_edit_original_response(
    interaction: discord.Interaction,
    *,
    content: Optional[str] = None,
    embed: Optional[discord.Embed] = None,
    view: Optional[discord.ui.View] = None,
):
    try:
        await interaction.edit_original_response(content=content, embed=embed, view=view)
        return True
    except Exception:
        return False


async def refresh_panel_message(message: Optional[discord.Message], guild_id: int):
    if not message:
        return
    try:
        await message.edit(view=TicketPanelView(guild_id))
    except Exception:
        pass


async def send_log(
    guild: discord.Guild,
    title: str,
    description: str,
    *,
    file: Optional[discord.File] = None,
):
    config = get_guild_config(guild.id)
    if not config:
        return

    channel = guild.get_channel(config["log_channel_id"])
    if not isinstance(channel, discord.TextChannel):
        return

    embed = base_embed(guild.id, title, description)
    try:
        if file:
            await channel.send(embed=embed, file=file)
        else:
            await channel.send(embed=embed)
    except Exception:
        logger.exception("Failed to send log to guild %s", guild.id)


async def send_error_log(guild: Optional[discord.Guild], where: str, exc: BaseException):
    if guild is None:
        logger.exception("Unhandled error in %s", where, exc_info=exc)
        return
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    logger.error("Guild %s error in %s:\n%s", guild.id, where, tb)

    trimmed = tb[-3500:] if len(tb) > 3500 else tb
    await send_log(guild, f"Internal Error - {where}", f"```py\n{trimmed}\n```")


async def dm_ticket_closed(guild: discord.Guild, opener_id: int, closed_by: discord.Member, channel_name: str):
    config = get_guild_config(guild.id)
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
        color=color,
    )
    embed.set_footer(text=FOOTER_TEXT, icon_url=bot_guild_avatar_url(guild.id))

    try:
        await user.send(embed=embed)
    except Exception:
        pass


# ============================================================
# TRANSCRIPTS
# ============================================================
async def fetch_channel_history_with_retry(channel: discord.TextChannel, attempts: int = 4) -> list[discord.Message]:
    last_error = None
    for attempt in range(attempts):
        try:
            return [m async for m in channel.history(limit=None, oldest_first=True)]
        except (discord.HTTPException, discord.DiscordServerError) as e:
            last_error = e
            await asyncio.sleep(1.25 * (attempt + 1))
        except Exception as e:
            last_error = e
            break
    raise last_error if last_error else RuntimeError("Failed to fetch channel history")


async def build_transcript_html(channel: discord.TextChannel) -> str:
    messages = await fetch_channel_history_with_retry(channel)
    rows: list[str] = []

    for msg in messages:
        created = format_ts(msg.created_at)
        author_name = html.escape(str(msg.author))
        author_id = msg.author.id
        avatar = html.escape(msg.author.display_avatar.url)

        content = html.escape(msg.content) if msg.content else ""
        content_html = content.replace("\n", "<br>") if content else '<span class="muted">No text content</span>'

        attachment_html = ""
        if msg.attachments:
            items = []
            for att in msg.attachments:
                att_name = html.escape(att.filename)
                att_url = html.escape(att.url)
                items.append(f'<li><a href="{att_url}" target="_blank">{att_name}</a></li>')
            attachment_html = f'<div class="attachments"><strong>Attachments</strong><ul>{"".join(items)}</ul></div>'

        embed_html = ""
        if msg.embeds:
            emb_parts = []
            for emb in msg.embeds:
                title_part = html.escape(emb.title) if emb.title else ""
                desc_part = html.escape(emb.description) if emb.description else ""
                inner = ""
                if title_part:
                    inner += f"<div><strong>{title_part}</strong></div>"
                if desc_part:
                    inner += f'<div class="embed-desc">{desc_part.replace(chr(10), "<br>")}</div>'
                if inner:
                    emb_parts.append(f'<div class="embed-box">{inner}</div>')
            if emb_parts:
                embed_html = f'<div class="embeds"><strong>Embeds</strong>{"".join(emb_parts)}</div>'

        rows.append(
            f'''
            <div class="message">
                <div class="message-head">
                    <img class="avatar" src="{avatar}" alt="avatar">
                    <div class="head-text">
                        <div class="meta-line"><span class="author">{author_name}</span> <span class="author-id">({author_id})</span></div>
                        <div class="time">{created}</div>
                    </div>
                </div>
                <div class="content">{content_html}</div>
                {attachment_html}
                {embed_html}
            </div>
            '''
        )

    guild_name = html.escape(channel.guild.name)
    channel_name = html.escape(channel.name)
    generated = format_ts(datetime.now(timezone.utc))

    return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Transcript - #{channel_name}</title>
<style>
body {{
    background: #0b0d12;
    color: #edf2f7;
    font-family: Inter, Arial, Helvetica, sans-serif;
    margin: 0;
    padding: 24px;
}}
.header {{
    margin-bottom: 24px;
    padding: 20px;
    background: #141925;
    border: 1px solid #2b3240;
    border-radius: 14px;
    box-shadow: 0 8px 30px rgba(0,0,0,0.25);
}}
.header h1 {{
    margin: 0 0 10px 0;
    font-size: 26px;
}}
.header .sub {{
    color: #b7c0cf;
    font-size: 14px;
    margin-top: 4px;
}}
.message {{
    background: #141925;
    border: 1px solid #2b3240;
    border-radius: 14px;
    padding: 16px;
    margin-bottom: 14px;
    box-shadow: 0 8px 22px rgba(0,0,0,0.18);
}}
.message-head {{
    display: flex;
    gap: 12px;
    align-items: center;
    margin-bottom: 12px;
}}
.avatar {{
    width: 42px;
    height: 42px;
    border-radius: 999px;
    object-fit: cover;
    border: 1px solid #334155;
}}
.meta-line {{
    font-size: 14px;
}}
.author {{
    color: #fff;
    font-weight: 700;
}}
.author-id {{
    color: #94a3b8;
    margin-left: 6px;
}}
.time {{
    color: #9aa7ba;
    font-size: 12px;
    margin-top: 4px;
}}
.content {{
    font-size: 15px;
    line-height: 1.6;
    word-wrap: break-word;
    white-space: normal;
}}
.attachments, .embeds {{
    margin-top: 12px;
    padding: 12px;
    background: #0f141d;
    border-radius: 10px;
    border: 1px solid #252c37;
}}
.embed-box {{
    margin-top: 8px;
    padding: 10px;
    border-left: 4px solid #8b5cf6;
    background: #171d28;
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
ul {{
    padding-left: 18px;
}}
</style>
</head>
<body>
<div class="header">
    <h1>Transcript for #{channel_name}</h1>
    <div class="sub">Guild: {guild_name} ({channel.guild.id})</div>
    <div class="sub">Channel ID: {channel.id}</div>
    <div class="sub">Generated: {generated}</div>
</div>
{''.join(rows) if rows else '<div class="message"><div class="content muted">No messages found.</div></div>'}
</body>
</html>'''


async def build_transcript_zip(channel: discord.TextChannel) -> discord.File:
    try:
        transcript_html = await build_transcript_html(channel)
    except Exception as e:
        transcript_html = f'''<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Transcript Error</title></head>
<body style="background:#111;color:#fff;font-family:Arial;padding:20px;">
<h1>Transcript could not be fully generated</h1>
<p>Channel: #{html.escape(channel.name)}</p>
<p>Guild: {html.escape(channel.guild.name)}</p>
<p>Error: {html.escape(str(e))}</p>
<p>Generated: {format_ts(datetime.now(timezone.utc))}</p>
</body>
</html>'''

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"transcript-{channel.name}.html", transcript_html)
    buf.seek(0)
    return discord.File(buf, filename=f"transcript-{channel.name}.zip")


# ============================================================
# ASSET STORAGE
# ============================================================
async def resolve_asset_storage_channel(guild: discord.Guild) -> Optional[discord.TextChannel]:
    if ASSET_CHANNEL_ID:
        ch = guild.get_channel(ASSET_CHANNEL_ID)
        if isinstance(ch, discord.TextChannel):
            return ch
    return None


async def persist_asset_for_guild(guild: discord.Guild, asset: StoredAsset) -> Optional[str]:
    storage_channel = await resolve_asset_storage_channel(guild)
    file = discord.File(io.BytesIO(asset.data), filename=asset.filename)

    try:
        if storage_channel:
            msg = await storage_channel.send(file=file)
            if msg.attachments:
                return msg.attachments[0].url
    except Exception:
        logger.exception("Failed to persist asset in storage channel for guild %s", guild.id)
    return None


# ============================================================
# GUILD PROFILE PATCH
# ============================================================
async def patch_guild_profile_with_retry(guild_id: int, payload: dict[str, Any], max_attempts: int = PROFILE_PATCH_MAX_ATTEMPTS) -> tuple[bool, str]:
    headers = {
        "Authorization": f"Bot {TOKEN}",
        "Content-Type": "application/json",
    }
    url = f"https://discord.com/api/v10/guilds/{guild_id}/members/@me"
    timeout = aiohttp.ClientTimeout(total=60)

    async with aiohttp.ClientSession(timeout=timeout) as session:
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
    avatar_attachment: Optional[discord.Attachment] = None,
    banner_attachment: Optional[discord.Attachment] = None,
) -> tuple[bool, str]:
    payload: dict[str, Any] = {}

    if nickname is not None:
        payload["nick"] = nickname

    if avatar_attachment is not None:
        if not is_image_attachment(avatar_attachment):
            return False, "Avatar attachment must be an image."
        raw = await avatar_attachment.read()
        mime = avatar_attachment.content_type or "image/png"
        b64 = base64.b64encode(raw).decode("utf-8")
        payload["avatar"] = f"data:{mime};base64,{b64}"

    if banner_attachment is not None:
        if not is_image_attachment(banner_attachment):
            return False, "Banner attachment must be an image."
        raw = await banner_attachment.read()
        mime = banner_attachment.content_type or "image/png"
        b64 = base64.b64encode(raw).decode("utf-8")
        payload["banner"] = f"data:{mime};base64,{b64}"

    if not payload:
        return False, "Nothing to update."

    return await patch_guild_profile_with_retry(guild_id, payload)


# ============================================================
# PROMPT HELPERS
# ============================================================
async def wait_for_user_message(channel: discord.TextChannel, user: discord.Member, timeout: int = MAX_WIZARD_WAIT) -> discord.Message:
    def check(m: discord.Message):
        return m.author.id == user.id and m.channel.id == channel.id and not m.author.bot
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
        if delete_reply:
            await safe_delete(reply)


async def ask_image(
    channel: discord.TextChannel,
    user: discord.Member,
    embed_builder,
    title: str,
    description: str,
) -> StoredAsset:
    while True:
        prompt = await channel.send(embed=embed_builder(title, description))
        reply = None
        try:
            reply = await wait_for_user_message(channel, user)
            if reply.content.strip().lower() in CANCEL_WORDS:
                raise SetupCancelled()

            if not reply.attachments:
                await channel.send(embed=embed_builder("No Image Found", "You need to upload an image in your reply."), delete_after=8)
                continue

            attachment = reply.attachments[0]
            if not is_image_attachment(attachment):
                await channel.send(embed=embed_builder("Invalid Image", "Attachment must be an image."), delete_after=8)
                continue

            raw = await attachment.read()
            return StoredAsset(filename=attachment.filename, data=raw, preview_url=attachment.url)
        finally:
            await safe_delete(prompt)


async def ask_channel_select(
    channel: discord.TextChannel,
    user: discord.Member,
    data: SetupData,
    title: str,
    description: str,
) -> discord.TextChannel:
    view = discord.ui.View(timeout=MAX_WIZARD_WAIT)
    select = discord.ui.ChannelSelect(
        placeholder="Choose a text channel",
        min_values=1,
        max_values=1,
        channel_types=[discord.ChannelType.text],
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
                timeout=MAX_WIZARD_WAIT,
            )
            values = (interaction.data or {}).get("values", [])
            if not values:
                await safe_send_interaction(interaction, embed=setup_embed(data, "Invalid Selection", "Please choose a text channel."), ephemeral=True)
                continue
            selected = channel.guild.get_channel(int(values[0]))
            if not isinstance(selected, discord.TextChannel):
                await safe_send_interaction(interaction, embed=setup_embed(data, "Invalid Selection", "That is not a valid text channel."), ephemeral=True)
                continue
            try:
                await interaction.response.edit_message(view=None)
            except Exception:
                pass
            return selected
    finally:
        try:
            await prompt.edit(view=None)
        except Exception:
            pass


async def ask_role_select(
    channel: discord.TextChannel,
    user: discord.Member,
    data: SetupData,
    title: str,
    description: str,
) -> discord.Role:
    view = discord.ui.View(timeout=MAX_WIZARD_WAIT)
    select = discord.ui.RoleSelect(placeholder="Choose a support role", min_values=1, max_values=1)
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
                timeout=MAX_WIZARD_WAIT,
            )
            values = (interaction.data or {}).get("values", [])
            if not values:
                await safe_send_interaction(interaction, embed=setup_embed(data, "Invalid Selection", "Please choose a role."), ephemeral=True)
                continue
            selected = channel.guild.get_role(int(values[0]))
            if not isinstance(selected, discord.Role):
                await safe_send_interaction(interaction, embed=setup_embed(data, "Invalid Selection", "That is not a valid role."), ephemeral=True)
                continue
            try:
                await interaction.response.edit_message(view=None)
            except Exception:
                pass
            return selected
    finally:
        try:
            await prompt.edit(view=None)
        except Exception:
            pass


async def ask_category_select(
    channel: discord.TextChannel,
    user: discord.Member,
    data: SetupData,
    title: str,
    description: str,
) -> discord.CategoryChannel:
    view = discord.ui.View(timeout=MAX_WIZARD_WAIT)
    select = discord.ui.ChannelSelect(
        placeholder="Choose a category",
        min_values=1,
        max_values=1,
        channel_types=[discord.ChannelType.category],
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
                timeout=MAX_WIZARD_WAIT,
            )
            values = (interaction.data or {}).get("values", [])
            if not values:
                await safe_send_interaction(interaction, embed=setup_embed(data, "Invalid Selection", "Please choose a category."), ephemeral=True)
                continue
            selected = channel.guild.get_channel(int(values[0]))
            if not isinstance(selected, discord.CategoryChannel):
                await safe_send_interaction(interaction, embed=setup_embed(data, "Invalid Selection", "That is not a valid category."), ephemeral=True)
                continue
            try:
                await interaction.response.edit_message(view=None)
            except Exception:
                pass
            return selected
    finally:
        try:
            await prompt.edit(view=None)
        except Exception:
            pass


# ============================================================
# WIZARDS
# ============================================================
async def run_setup_wizard(interaction: discord.Interaction):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return

    guild = interaction.guild
    user = interaction.user
    channel = interaction.channel

    if not isinstance(channel, discord.TextChannel):
        await interaction.followup.send(embed=base_embed(guild.id, "Setup Failed", "Setup must be run in a text channel.", error=True), ephemeral=True)
        return

    data = SetupData(guild_id=guild.id, user_id=user.id, setup_channel_id=channel.id)
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
            channel,
            user,
            embed_builder,
            "Embed Color",
            "Send the embed hex color.\nExample: `#00FF66`\n\nType `skip` to use the default color.",
            optional=True,
        )
        if color_raw:
            try:
                data.color_hex = normalize_hex(color_raw)
            except ValueError:
                data.color_hex = DEFAULT_COLOR
                await channel.send(embed=base_embed(guild.id, "Invalid Color", f"Invalid hex color. Default color `{DEFAULT_COLOR}` will be used.", error=True), delete_after=8)

        panel_channel = await ask_channel_select(channel, user, data, "Panel Channel", "Choose the channel where the ticket panel should be posted.")
        data.panel_channel_id = panel_channel.id

        support_role = await ask_role_select(channel, user, data, "Support Team Role", "Choose the support team role.")
        data.support_role_id = support_role.id

        data.option_1_name = await ask_text(channel, user, embed_builder, "Ticket Option 1 Name", "Send the name for the first ticket option.\nExample: `Support Ticket`")
        option_1_category = await ask_category_select(channel, user, data, "Ticket Option 1 Category", "Choose the category for ticket option 1.")
        data.option_1_category_id = option_1_category.id

        data.option_2_name = await ask_text(channel, user, embed_builder, "Ticket Option 2 Name", "Send the name for the second ticket option, or type `skip`.", optional=True)
        if data.option_2_name:
            option_2_category = await ask_category_select(channel, user, data, "Ticket Option 2 Category", "Choose the category for ticket option 2.")
            data.option_2_category_id = option_2_category.id

        data.option_3_name = await ask_text(channel, user, embed_builder, "Ticket Option 3 Name", "Send the name for the third ticket option, or type `skip`.", optional=True)
        if data.option_3_name:
            option_3_category = await ask_category_select(channel, user, data, "Ticket Option 3 Category", "Choose the category for ticket option 3.")
            data.option_3_category_id = option_3_category.id

        log_channel = await ask_channel_select(channel, user, data, "Log Channel", "Choose the log channel.")
        data.log_channel_id = log_channel.id

        data.banner = await ask_image(channel, user, embed_builder, "Banner", "Reply with the banner image uploaded as an attachment.")
        data.thumbnail = await ask_image(channel, user, embed_builder, "Server PFP / Small Picture", "Reply with the small picture image uploaded as an attachment.")

        await channel.send(embed=build_setup_preview_embed(guild, data), view=SetupConfirmView(data))
        await channel.send(embed=setup_embed(data, "Setup Ready", "Review the preview above and click **Publish** or **Cancel**."), delete_after=20)

    except SetupCancelled:
        await channel.send(embed=setup_embed(data, "Setup Cancelled", "The ticket setup was cancelled."))
        cleanup_setup(guild.id, user.id)
    except Exception as e:
        await send_error_log(guild, "run_setup_wizard", e)
        await channel.send(embed=base_embed(guild.id, "Setup Failed", f"An error happened during setup:\n`{e}`", error=True))
        cleanup_setup(guild.id, user.id)


async def run_setprofile_wizard(interaction: discord.Interaction):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return

    guild = interaction.guild
    user = interaction.user
    channel = interaction.channel

    if not isinstance(channel, discord.TextChannel):
        await interaction.followup.send(embed=base_embed(guild.id, "Set Profile Failed", "This command must be run in a text channel.", error=True), ephemeral=True)
        return

    data = SetProfileData(guild_id=guild.id, user_id=user.id, channel_id=channel.id)
    setprofile_sessions[(guild.id, user.id)] = data
    embed_builder = lambda t, d: base_embed(guild.id, t, d)

    try:
        await channel.send(embed=base_embed(guild.id, "Set Profile Started", "I will ask you 3 questions.\n\nReply in this channel.\nType `cancel` anytime to stop."))

        data.display = await ask_text(channel, user, embed_builder, "Display", "Send the display name the bot should have in this server.")

        while True:
            prompt = await channel.send(embed=base_embed(guild.id, "Profile Picture", "Reply with the image that should be the bot profile picture in this server."))
            reply = None
            try:
                reply = await wait_for_user_message(channel, user)
                if reply.content.strip().lower() in CANCEL_WORDS:
                    raise SetupCancelled()
                if not reply.attachments:
                    await channel.send(embed=base_embed(guild.id, "No Image Found", "You need to upload an image."), delete_after=8)
                    continue
                if not is_image_attachment(reply.attachments[0]):
                    await channel.send(embed=base_embed(guild.id, "Invalid Image", "Attachment must be an image."), delete_after=8)
                    continue
                data.avatar_attachment = reply.attachments[0]
                break
            finally:
                await safe_delete(prompt)

        while True:
            prompt = await channel.send(embed=base_embed(guild.id, "Banner", "Reply with the image that should be the bot banner in this server."))
            reply = None
            try:
                reply = await wait_for_user_message(channel, user)
                if reply.content.strip().lower() in CANCEL_WORDS:
                    raise SetupCancelled()
                if not reply.attachments:
                    await channel.send(embed=base_embed(guild.id, "No Image Found", "You need to upload an image."), delete_after=8)
                    continue
                if not is_image_attachment(reply.attachments[0]):
                    await channel.send(embed=base_embed(guild.id, "Invalid Image", "Attachment must be an image."), delete_after=8)
                    continue
                data.banner_attachment = reply.attachments[0]
                break
            finally:
                await safe_delete(prompt)

        success, message = await set_guild_profile(
            guild_id=guild.id,
            nickname=data.display,
            avatar_attachment=data.avatar_attachment,
            banner_attachment=data.banner_attachment,
        )

        if success:
            await channel.send(embed=base_embed(guild.id, "Server Profile Updated", "The bot server profile was updated in this server."))
        else:
            await channel.send(embed=base_embed(guild.id, "Update Failed", message, error=True))

        cleanup_setprofile(guild.id, user.id)
    except SetupCancelled:
        await channel.send(embed=base_embed(guild.id, "Set Profile Cancelled", "The server profile update was cancelled."))
        cleanup_setprofile(guild.id, user.id)
    except Exception as e:
        await send_error_log(guild, "run_setprofile_wizard", e)
        await channel.send(embed=base_embed(guild.id, "Set Profile Failed", f"An error happened:\n`{e}`", error=True))
        cleanup_setprofile(guild.id, user.id)


# ============================================================
# PERSISTENT VIEWS
# ============================================================
class SetupConfirmView(discord.ui.View):
    def __init__(self, data: SetupData):
        super().__init__(timeout=900)
        self.data = data

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.data.user_id:
            await safe_send_interaction(
                interaction,
                embed=base_embed(interaction.guild.id if interaction.guild else None, "Access Denied", "This setup is not yours.", error=True),
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(label="Publish", style=discord.ButtonStyle.success)
    async def publish_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.guild:
            return

        await safe_defer(interaction, ephemeral=True)
        guild = interaction.guild
        data = self.data

        try:
            panel_channel = guild.get_channel(data.panel_channel_id)
            if not isinstance(panel_channel, discord.TextChannel):
                await safe_edit_original_response(interaction, embed=base_embed(guild.id, "Publish Failed", "Panel channel is invalid.", error=True))
                return

            # Save options first
            await clear_ticket_options(guild.id)
            await save_ticket_option(guild.id, 1, data.option_1_name, data.option_1_category_id)
            if data.option_2_name and data.option_2_category_id:
                await save_ticket_option(guild.id, 2, data.option_2_name, data.option_2_category_id)
            if data.option_3_name and data.option_3_category_id:
                await save_ticket_option(guild.id, 3, data.option_3_name, data.option_3_category_id)

            # Persist assets into asset channel if available, fallback to panel attachments
            banner_url = None
            thumbnail_url = None

            if data.banner:
                banner_url = await persist_asset_for_guild(guild, data.banner)
            if data.thumbnail:
                thumbnail_url = await persist_asset_for_guild(guild, data.thumbnail)

            embed = discord.Embed(title=data.title, description=data.description, color=hex_to_color(data.color_hex))
            embed.set_footer(text=FOOTER_TEXT, icon_url=bot_guild_avatar_url(guild.id))

            files: list[discord.File] = []
            if data.thumbnail:
                if thumbnail_url:
                    embed.set_thumbnail(url=thumbnail_url)
                else:
                    files.append(discord.File(io.BytesIO(data.thumbnail.data), filename=data.thumbnail.filename))
                    embed.set_thumbnail(url=f"attachment://{data.thumbnail.filename}")

            if data.banner:
                if banner_url:
                    embed.set_image(url=banner_url)
                else:
                    files.append(discord.File(io.BytesIO(data.banner.data), filename=data.banner.filename))
                    embed.set_image(url=f"attachment://{data.banner.filename}")

            panel_message = await panel_channel.send(embed=embed, files=files, view=TicketPanelView(guild.id))

            if panel_message.attachments:
                for att in panel_message.attachments:
                    if data.banner and att.filename == data.banner.filename and not banner_url:
                        banner_url = att.url
                    if data.thumbnail and att.filename == data.thumbnail.filename and not thumbnail_url:
                        thumbnail_url = att.url

            await save_guild_config(
                guild_id=guild.id,
                panel_channel_id=panel_channel.id,
                panel_message_id=panel_message.id,
                title=data.title,
                description=data.description,
                color_hex=data.color_hex,
                banner_url=banner_url,
                thumbnail_url=thumbnail_url,
                support_role_id=data.support_role_id,
                log_channel_id=data.log_channel_id,
            )

            bot.add_view(TicketPanelView(guild.id), message_id=panel_message.id)

            try:
                if interaction.message:
                    await interaction.message.edit(embed=base_embed(guild.id, "Setup Complete", f"Ticket panel created in {panel_channel.mention}."), view=None)
            except Exception:
                pass

            await safe_edit_original_response(interaction, embed=base_embed(guild.id, "Setup Complete", f"Ticket panel created in {panel_channel.mention}."))
            cleanup_setup(guild.id, interaction.user.id)
        except Exception as e:
            await send_error_log(guild, "SetupConfirmView.publish_button", e)
            await safe_edit_original_response(interaction, embed=base_embed(guild.id, "Publish Failed", f"An error happened while publishing the setup.\n`{e}`", error=True))

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await safe_defer(interaction, ephemeral=True)
        try:
            if interaction.message:
                await interaction.message.edit(embed=base_embed(interaction.guild.id if interaction.guild else None, "Setup Cancelled", "The setup was cancelled."), view=None)
        except Exception:
            pass

        if interaction.guild:
            cleanup_setup(interaction.guild.id, interaction.user.id)

        await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id if interaction.guild else None, "Setup Cancelled", "The setup was cancelled."))


class TicketDropdown(discord.ui.Select):
    def __init__(self, guild_id: int):
        rows = get_ticket_options(guild_id)
        options = [discord.SelectOption(label=row["label"][:100], value=str(row["option_index"])) for row in rows]
        super().__init__(
            placeholder="Make a selection",
            min_values=1,
            max_values=1,
            options=options,
            custom_id=f"ticket_dropdown:{guild_id}",
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return

        guild = interaction.guild
        opener = interaction.user

        await safe_defer(interaction, ephemeral=True)

        try:
            config = get_guild_config(guild.id)
            if not config:
                await safe_edit_original_response(interaction, embed=base_embed(guild.id, "Error", "Ticket system is not configured.", error=True))
                return

            await refresh_panel_message(interaction.message, guild.id)

            create_lock = get_ticket_create_lock(guild.id, opener.id)
            async with create_lock:
                existing = await get_open_ticket_for_user(guild.id, opener.id)
                if existing:
                    existing_channel = guild.get_channel(existing["channel_id"])
                    if existing_channel:
                        await safe_edit_original_response(interaction, embed=base_embed(guild.id, "Open Ticket Found", f"You already have an open ticket: {existing_channel.mention}"))
                    else:
                        await safe_edit_original_response(interaction, embed=base_embed(guild.id, "Open Ticket Found", "You already have an open ticket."))
                    return

                try:
                    selected_index = int(self.values[0])
                except Exception:
                    await safe_edit_original_response(interaction, embed=base_embed(guild.id, "Error", "Invalid selection.", error=True))
                    return

                rows = get_ticket_options(guild.id)
                selected = next((r for r in rows if r["option_index"] == selected_index), None)
                if not selected:
                    await safe_edit_original_response(interaction, embed=base_embed(guild.id, "Error", "That ticket option is no longer configured.", error=True))
                    return

                category = guild.get_channel(selected["category_id"])
                if not isinstance(category, discord.CategoryChannel):
                    await safe_edit_original_response(interaction, embed=base_embed(guild.id, "Error", "The configured category is invalid.", error=True))
                    return

                support_role = guild.get_role(config["support_role_id"])
                if not support_role:
                    await safe_edit_original_response(interaction, embed=base_embed(guild.id, "Error", "The support role is invalid.", error=True))
                    return

                me = guild.me or guild.get_member(bot.user.id) if bot.user else None
                if me is None:
                    await safe_edit_original_response(interaction, embed=base_embed(guild.id, "Error", "Bot member could not be resolved in this server.", error=True))
                    return

                base_name = clean_channel_name(f"{selected['label']}-{opener.name}")
                channel_name = base_name
                existing_names = {c.name for c in guild.channels}
                counter = 2
                while channel_name in existing_names:
                    suffix = f"-{counter}"
                    channel_name = f"{base_name[:90 - len(suffix)]}{suffix}"
                    counter += 1

                overwrites = {
                    guild.default_role: discord.PermissionOverwrite(view_channel=False),
                    opener: discord.PermissionOverwrite(
                        view_channel=True,
                        send_messages=True,
                        read_message_history=True,
                        attach_files=True,
                        embed_links=True,
                    ),
                    support_role: discord.PermissionOverwrite(
                        view_channel=True,
                        send_messages=True,
                        read_message_history=True,
                        attach_files=True,
                        embed_links=True,
                        manage_messages=True,
                    ),
                    me: discord.PermissionOverwrite(
                        view_channel=True,
                        send_messages=True,
                        read_message_history=True,
                        attach_files=True,
                        embed_links=True,
                        manage_channels=True,
                        manage_messages=True,
                        manage_permissions=True,
                    ),
                }

                ticket_channel = None
                last_error = None
                for attempt in range(CHANNEL_CREATE_RETRY_ATTEMPTS):
                    try:
                        ticket_channel = await guild.create_text_channel(
                            name=channel_name,
                            category=category,
                            overwrites=overwrites,
                            reason=f"Ticket created by {opener} ({opener.id})",
                        )
                        break
                    except discord.Forbidden as e:
                        last_error = e
                        break
                    except discord.HTTPException as e:
                        last_error = e
                        await asyncio.sleep(0.8 * (attempt + 1))

                if ticket_channel is None:
                    if isinstance(last_error, discord.Forbidden):
                        await safe_edit_original_response(interaction, embed=base_embed(guild.id, "Error", "I do not have permission to create channels in that category.", error=True))
                    else:
                        await safe_edit_original_response(interaction, embed=base_embed(guild.id, "Error", f"Failed to create the ticket channel.\n`{last_error}`", error=True))
                    return

                await create_ticket_record(ticket_channel.id, guild.id, opener.id, selected["label"])

                try:
                    await ticket_channel.send(
                        content=f"{support_role.mention} {opener.mention}",
                        embed=build_ticket_embed(guild.id, selected["label"], opener),
                        view=TicketControlsView(),
                    )
                except Exception:
                    logger.exception("Failed to send opening message in ticket channel %s", ticket_channel.id)

                await send_log(guild, "Ticket Opened", f"User: {opener.mention}\nChannel: {ticket_channel.mention}\nType: {selected['label']}")
                await safe_edit_original_response(interaction, embed=base_embed(guild.id, "Ticket Created", f"Your ticket has been created: {ticket_channel.mention}"))
        except Exception as e:
            await send_error_log(guild, "TicketDropdown.callback", e)
            await safe_edit_original_response(interaction, embed=base_embed(guild.id, "Error", f"An unexpected error happened.\n`{e}`", error=True))


class TicketPanelView(discord.ui.View):
    def __init__(self, guild_id: int):
        super().__init__(timeout=None)
        rows = get_ticket_options(guild_id)
        if rows:
            self.add_item(TicketDropdown(guild_id))


class ClaimTicketButton(discord.ui.Button):
    def __init__(self, disabled: bool = False, *, closed_variant: bool = False):
        super().__init__(
            label="Claim Ticket",
            style=discord.ButtonStyle.secondary,
            custom_id="ticket_claim_button_closed" if closed_variant else "ticket_claim_button",
            disabled=disabled,
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            return

        await safe_defer(interaction, ephemeral=True)
        lock = get_ticket_channel_lock(interaction.channel.id)

        async with lock:
            try:
                ticket = await get_ticket_by_channel(interaction.channel.id)
                if not ticket:
                    await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Error", "This is not a tracked ticket channel.", error=True))
                    return

                if ticket["status"] == "closed":
                    await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Ticket Closed", "This ticket is already closed."))
                    return

                if not is_support_or_admin(interaction.user, interaction.guild.id):
                    await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Access Denied", "Only the support team or admins can claim tickets.", error=True))
                    return

                if ticket["claimed_by"] == interaction.user.id:
                    await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Already Claimed", "You already claimed this ticket."))
                    return

                await set_ticket_claimed(interaction.channel.id, interaction.user.id)

                try:
                    await interaction.channel.send(embed=base_embed(interaction.guild.id, "Ticket Claimed", f"Ticket claimed by {interaction.user.mention}."))
                except Exception:
                    pass

                await send_log(interaction.guild, "Ticket Claimed", f"Channel: {interaction.channel.mention}\nClaimed by: {interaction.user.mention}")
                await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Claim Saved", "You claimed this ticket."))
            except Exception as e:
                await send_error_log(interaction.guild, "ClaimTicketButton.callback", e)
                await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Error", f"An error happened while claiming the ticket.\n`{e}`", error=True))


class CloseTicketButton(discord.ui.Button):
    def __init__(self, disabled: bool = False, *, closed_variant: bool = False):
        super().__init__(
            label="Close Ticket",
            style=discord.ButtonStyle.danger,
            custom_id="ticket_close_button_closed" if closed_variant else "ticket_close_button",
            disabled=disabled,
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            return

        await safe_defer(interaction, ephemeral=True)
        lock = get_ticket_channel_lock(interaction.channel.id)

        async with lock:
            try:
                ticket = await get_ticket_by_channel(interaction.channel.id)
                if not ticket:
                    await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Error", "This is not a tracked ticket channel.", error=True))
                    return

                if ticket["status"] == "closed":
                    await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Already Closed", "This ticket is already closed."))
                    return

                if not is_support_or_admin(interaction.user, interaction.guild.id):
                    await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Access Denied", "Only the support team or admins can close tickets.", error=True))
                    return

                config = get_guild_config(interaction.guild.id)
                support_role = interaction.guild.get_role(config["support_role_id"]) if config else None
                opener_member = await try_fetch_member(interaction.guild, ticket["opener_id"])

                if opener_member:
                    try:
                        await interaction.channel.set_permissions(opener_member, view_channel=False)
                    except discord.Forbidden:
                        await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Error", "I do not have permission to update opener permissions.", error=True))
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
                            manage_messages=True,
                        )
                    except discord.Forbidden:
                        await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Error", "I do not have permission to update support role permissions.", error=True))
                        return

                await close_ticket_record(interaction.channel.id, interaction.user.id)

                try:
                    if interaction.message:
                        await interaction.message.edit(view=ClosedTicketControlsView())
                except Exception:
                    pass

                try:
                    await interaction.channel.send(embed=build_closed_ticket_embed(interaction.guild.id, interaction.user))
                except Exception:
                    pass

                await dm_ticket_closed(interaction.guild, ticket["opener_id"], interaction.user, interaction.channel.name)
                await send_log(interaction.guild, "Ticket Closed", f"Channel: #{interaction.channel.name}\nClosed by: {interaction.user.mention}")
                await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Ticket Closed", "Ticket closed successfully."))
            except Exception as e:
                await send_error_log(interaction.guild, "CloseTicketButton.callback", e)
                await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Error", f"An error happened while closing the ticket.\n`{e}`", error=True))


class DeleteTicketButton(discord.ui.Button):
    def __init__(self, *, closed_variant: bool = False):
        super().__init__(
            label="Delete Ticket",
            style=discord.ButtonStyle.danger,
            custom_id="ticket_delete_button_closed" if closed_variant else "ticket_delete_button",
        )

    async def callback(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return
        if not isinstance(interaction.channel, discord.TextChannel):
            return

        await safe_defer(interaction, ephemeral=True)
        lock = get_ticket_channel_lock(interaction.channel.id)

        async with lock:
            try:
                ticket = await get_ticket_by_channel(interaction.channel.id)
                if not ticket:
                    await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Error", "This is not a tracked ticket channel.", error=True))
                    return

                if not is_support_or_admin(interaction.user, interaction.guild.id):
                    await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Access Denied", "Only the support team or admins can delete tickets.", error=True))
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
                    file=transcript_file,
                )

                await delete_ticket_record(interaction.channel.id)
                await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Deleting Ticket", "The ticket is being deleted."))
                await asyncio.sleep(DELETE_DELAY_SECONDS)

                try:
                    await interaction.channel.delete(reason=f"Ticket deleted by {interaction.user}")
                except Exception:
                    logger.exception("Failed to delete channel %s after delete command", interaction.channel.id)
            except Exception as e:
                await send_error_log(interaction.guild, "DeleteTicketButton.callback", e)
                await safe_edit_original_response(interaction, embed=base_embed(interaction.guild.id, "Error", f"An error happened while deleting the ticket.\n`{e}`", error=True))


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


# ============================================================
# COMMANDS
# ============================================================
@bot.tree.command(name="setup", description="Start the guided ticket setup")
@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
async def setup(interaction: discord.Interaction):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return

    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(embed=base_embed(None, "Access Denied", "Only users with Administrator can use this command.", error=True), ephemeral=True)
        return

    if interaction.guild.id in active_setup_guilds:
        await interaction.response.send_message(embed=base_embed(interaction.guild.id, "Setup Running", "A setup is already running in this server."), ephemeral=True)
        return

    active_setup_guilds.add(interaction.guild.id)
    await interaction.response.send_message(embed=base_embed(interaction.guild.id, "Setup Started", "The guided setup has started in this channel."), ephemeral=True)
    await run_setup_wizard(interaction)


@bot.tree.command(name="setprofile", description="Set the bot display, profile picture and banner for this server")
@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
async def setprofile(interaction: discord.Interaction):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return

    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message(embed=base_embed(None, "Access Denied", "Only users with Administrator can use this command.", error=True), ephemeral=True)
        return

    if interaction.guild.id in active_setprofile_guilds:
        await interaction.response.send_message(embed=base_embed(interaction.guild.id, "Set Profile Running", "A setprofile session is already running in this server."), ephemeral=True)
        return

    active_setprofile_guilds.add(interaction.guild.id)
    await interaction.response.send_message(embed=base_embed(interaction.guild.id, "Set Profile Started", "The guided server profile setup has started in this channel."), ephemeral=True)
    await run_setprofile_wizard(interaction)


@bot.tree.command(name="remind", description="DM a user to reply in their ticket")
@app_commands.guild_only()
async def remind(interaction: discord.Interaction, user: discord.Member, message: str):
    if not interaction.guild or not isinstance(interaction.user, discord.Member):
        return

    if not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message(embed=base_embed(interaction.guild.id, "Error", "This command must be used inside a ticket channel.", error=True), ephemeral=True)
        return

    if not is_support_or_admin(interaction.user, interaction.guild.id):
        await interaction.response.send_message(embed=base_embed(interaction.guild.id, "Access Denied", "Only the support team or admins can use this command.", error=True), ephemeral=True)
        return

    ticket = await get_ticket_by_channel(interaction.channel.id)
    if not ticket:
        await interaction.response.send_message(embed=base_embed(interaction.guild.id, "Error", "This command can only be used inside a ticket channel.", error=True), ephemeral=True)
        return

    if ticket["opener_id"] != user.id:
        await interaction.response.send_message(embed=base_embed(interaction.guild.id, "Error", "That user is not the opener of this ticket.", error=True), ephemeral=True)
        return

    config = get_guild_config(interaction.guild.id)
    color = hex_to_color(config["color_hex"]) if config else discord.Color.green()
    dm_embed = discord.Embed(
        title="Ticket Reminder",
        description=(
            f"You have an open ticket in **{interaction.guild.name}**.\n\n"
            f"Message from support:\n{message}\n\n"
            f"Ticket channel: #{interaction.channel.name}"
        ),
        color=color,
    )
    dm_embed.set_footer(text=FOOTER_TEXT, icon_url=bot_guild_avatar_url(interaction.guild.id))

    try:
        await user.send(embed=dm_embed)
    except discord.Forbidden:
        await interaction.response.send_message(embed=base_embed(interaction.guild.id, "DM Failed", "I could not DM that user.", error=True), ephemeral=True)
        return

    await interaction.response.send_message(embed=base_embed(interaction.guild.id, "Reminder Sent", f"Reminder sent to {user.mention}."), ephemeral=True)
    await send_log(
        interaction.guild,
        "Ticket Reminder Sent",
        f"Channel: {interaction.channel.mention}\nTo: {user.mention}\nBy: {interaction.user.mention}\nMessage: {message}",
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
        await interaction.response.send_message(embed=base_embed(None, "Access Denied", "Only users with Administrator can use this command.", error=True), ephemeral=True)
        return

    if avatar is not None and not is_image_attachment(avatar):
        await interaction.response.send_message(embed=base_embed(None, "Update Failed", "Avatar attachment must be an image.", error=True), ephemeral=True)
        return

    success, message = await set_guild_profile(
        guild_id=interaction.guild.id,
        nickname=nickname,
        avatar_attachment=avatar,
        banner_attachment=None,
    )

    if success:
        await interaction.response.send_message(embed=base_embed(interaction.guild.id, "Server Profile Updated", "The bot profile was updated for this server."), ephemeral=True)
    else:
        await interaction.response.send_message(embed=base_embed(interaction.guild.id, "Update Failed", message, error=True), ephemeral=True)


# ============================================================
# COMMAND ERRORS
# ============================================================
@setup.error
async def setup_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if interaction.guild:
        cleanup_setup(interaction.guild.id, interaction.user.id)
    embed = base_embed(interaction.guild.id if interaction.guild else None, "Setup Failed", f"{error}", error=True)
    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


@setprofile.error
async def setprofile_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if interaction.guild:
        cleanup_setprofile(interaction.guild.id, interaction.user.id)
    embed = base_embed(interaction.guild.id if interaction.guild else None, "Set Profile Failed", f"{error}", error=True)
    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


@remind.error
async def remind_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    embed = base_embed(interaction.guild.id if interaction.guild else None, "Remind Failed", f"{error}", error=True)
    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


@serverprofile.error
async def serverprofile_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    embed = base_embed(interaction.guild.id if interaction.guild else None, "Update Failed", f"{error}", error=True)
    if interaction.response.is_done():
        await interaction.followup.send(embed=embed, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed, ephemeral=True)


# ============================================================
# EVENTS
# ============================================================
@bot.event
async def on_ready():
    global command_tree_synced

    async with startup_once_lock:
        if SYNC_COMMAND_TREE and not command_tree_synced:
            try:
                synced = await bot.tree.sync()
                logger.info("Synced %s commands", len(synced))
                command_tree_synced = True
            except Exception:
                logger.exception("Command sync failed")

        try:
            bot.add_view(TicketControlsView())
            bot.add_view(ClosedTicketControlsView())
        except Exception:
            pass

        try:
            rows = await get_all_panel_rows()
            for row in rows:
                try:
                    bot.add_view(TicketPanelView(int(row["guild_id"])), message_id=int(row["panel_message_id"]))
                except Exception:
                    logger.exception("Failed to restore panel view for guild %s", row["guild_id"])
        except Exception:
            logger.exception("Failed restoring persistent panel views")

    logger.info("Logged in as %s (%s)", bot.user, bot.user.id if bot.user else None)


@bot.event
async def on_guild_channel_delete(channel: discord.abc.GuildChannel):
    if not isinstance(channel, discord.TextChannel):
        return
    try:
        ticket = await get_ticket_by_channel(channel.id)
        if ticket:
            await delete_ticket_record(channel.id)
    except Exception:
        logger.exception("Failed cleanup after channel delete: %s", channel.id)


@bot.event
async def on_error(event_method: str, *args: Any, **kwargs: Any):
    logger.exception("Unhandled event error in %s", event_method)


# ============================================================
# MAIN
# ============================================================
async def main():
    await create_db_pool()
    await init_db()
    await load_cache()

    async with bot:
        await bot.start(TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
