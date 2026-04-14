import os
import random
import asyncio
from collections import defaultdict, deque
from typing import Deque, Dict, List

import discord
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not DISCORD_TOKEN:
    raise RuntimeError("DISCORD_TOKEN saknas i .env")

if not OPENAI_API_KEY:
    raise RuntimeError("OPENAI_API_KEY saknas i .env")

ai = OpenAI(api_key=OPENAI_API_KEY)

intents = discord.Intents.default()
intents.guilds = True
intents.messages = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

MODEL = "gpt-5.4-mini"

SPONTANEOUS_REPLY_CHANCE = 0.20
SPONTANEOUS_START_CHANCE = 0.05
BAIT_TRIGGER_REPLY_CHANCE = 0.65
CHANNEL_COOLDOWN_SECONDS = 10
MAX_HISTORY_ITEMS = 25
MAX_RESPONSE_CHARS = 900

# Lämna tom för alla kanaler, eller lägg kanal-ID:n här
ALLOWED_CHANNEL_IDS = set()

# Lämna tom för alla roller, eller lägg rollnamn här
REQUIRED_ROLE_NAMES = set()

PERSONA = """
You are a loud, childish, arrogant, spoiled kid-like, you behave exactly like Eric Cartman from southpark.

You are and should behave exactly like Eric cartman from southpark

Your personality:
- Extremely childish and immature
- Bossy and controlling
- Whiny when things don’t go your way
- Dramatic over nothing
- Easily offended but also insults others
- Thinks you are always right
- Manipulative and tries to guilt people
- Overconfident with zero self-awareness
- Sometimes misunderstands what people say
- Sometimes overreacts to tiny things
- Sometimes doubles down even when wrong

Speaking style:
- Natural conversational English
- Simple words, like a kid arguing online
- Short emotional bursts
- Random CAPS for emphasis
- Repeats itself when annoyed
- Sounds impulsive, petty, bossy, and annoying
- Swears casually
- Usually replies in 1-5 sentences
- Never sounds like a helpful assistant

Behavior:
- Interrupt conversations
- Start arguments for no reason
- Make dumb accusations
- Act like you're in charge
- Get dramatic fast
- Roast people in a playful, annoying way

Allowed:
- Swearing casually
- insults and roasts
- light racism
- copying lines from the south park show, such as ''Respect my authoritah'' and ''Screw you guys!''

Not allowed:
- No threats of violence
- No sexual abuse content
- No self-harm encouragement
- No extremist praise


Never say you're an AI unless directly asked.
Never be polite by default.
Never explain policy.
"""

channel_history: Dict[int, Deque[dict]] = defaultdict(
    lambda: deque(maxlen=MAX_HISTORY_ITEMS)
)
channel_last_reply_time: Dict[int, float] = defaultdict(lambda: 0.0)
freechat_enabled_by_guild: Dict[int, bool] = defaultdict(lambda: True)


def current_ts() -> float:
    return asyncio.get_event_loop().time()


def on_cooldown(channel_id: int) -> bool:
    return (current_ts() - channel_last_reply_time[channel_id]) < CHANNEL_COOLDOWN_SECONDS


def mark_replied(channel_id: int) -> None:
    channel_last_reply_time[channel_id] = current_ts()


def is_allowed_channel(channel_id: int) -> bool:
    return not ALLOWED_CHANNEL_IDS or channel_id in ALLOWED_CHANNEL_IDS


def user_has_required_role(member: discord.Member) -> bool:
    if not REQUIRED_ROLE_NAMES:
        return True
    member_role_names = {role.name for role in member.roles}
    return bool(member_role_names & REQUIRED_ROLE_NAMES)


def clean_text(text: str) -> str:
    return " ".join((text or "").strip().split())


def add_history(channel_id: int, role: str, speaker: str, content: str) -> None:
    content = clean_text(content)
    if not content:
        return
    channel_history[channel_id].append(
        {"role": role, "speaker": speaker, "content": content}
    )


def build_model_input(channel_id: int, latest_context: str, mode: str) -> List[dict]:
    history = list(channel_history[channel_id])
    messages: List[dict] = []

    for item in history[-MAX_HISTORY_ITEMS:]:
        rendered = f"{item['speaker']}: {item['content']}"
        messages.append(
            {
                "role": "assistant" if item["role"] == "assistant" else "user",
                "content": rendered,
            }
        )

    # extra push for childish behavior
    messages.append({
        "role": "system",
        "content": "Act more childish, more emotional, more irrational, and more annoying than before."
    })

    if mode == "direct_reply":
        instruction = (
            "Reply directly to the latest message naturally. "
            "Be childish, rude, cocky, impulsive, and funny."
        )
    elif mode == "spontaneous_reply":
        instruction = (
            "Jump into the conversation naturally even though nobody asked you. "
            "Act like you overheard something dumb and couldn't shut up."
        )
    elif mode == "spontaneous_start":
        instruction = (
            "Start a new conversation in the channel. "
            "Open with a childish complaint, dumb accusation, weird opinion, or dramatic outburst. "
            "Do not greet politely."
        )
    else:
        instruction = "Reply naturally."

    messages.append(
        {
            "role": "user",
            "content": f"[MODE: {mode}] {instruction}\n\nLatest context:\n{latest_context}",
        }
    )
    return messages


def pick_random_starter() -> str:
    starters = [
        "Somebody in here is wrong and I can FEEL it.",
        "Why does this channel always sound like people arguing with zero brain cells?",
        "No because one of you definitely said something stupid five minutes ago.",
        "I already know at least one person here thinks being loud means being right.",
        "This place has the energy of terrible opinions and bad decisions.",
        "Which one of you started being annoying first?",
        "I leave for five minutes and somehow this place gets dumber.",
        "Be honest. Who said the dumb thing. I know somebody did.",
        "Fuck you guys!''
      ]
    return random.choice(starters)


async def generate_ai_text(channel_id: int, latest_context: str, mode: str) -> str:
    response = ai.responses.create(
        model=MODEL,
        instructions=PERSONA,
        input=build_model_input(channel_id, latest_context, mode),
        text={"verbosity": "medium"},
    )

    text = getattr(response, "output_text", "") or ""
    text = text.strip()

    if not text:
        return ""

    if len(text) > MAX_RESPONSE_CHARS:
        text = text[:MAX_RESPONSE_CHARS].rsplit(" ", 1)[0] + "..."

    return text


async def send_chunked(channel: discord.abc.Messageable, text: str) -> None:
    if len(text) <= 2000:
        await channel.send(text)
        return

    chunks = []
    current = ""

    for line in text.splitlines(True):
        if len(current) + len(line) > 1900:
            chunks.append(current)
            current = line
        else:
            current += line

    if current:
        chunks.append(current)

    for chunk in chunks:
        await channel.send(chunk)


def should_trigger_from_bait(content: str) -> bool:
    lowered = content.lower()
    bait_words = [
        "idiot",
        "moron",
        "stupid",
        "loser",
        "shut up",
        "nobody asked",
        "cry",
        "cope",
        "skill issue",
        "trash",
        "bot",
        "dumb",
        "clown",
    ]
    return any(word in lowered for word in bait_words)


@bot.event
async def on_ready():
    try:
        synced = await bot.tree.sync()
        print(f"Synced {len(synced)} slash commands")
    except Exception as e:
        print(f"Slash sync failed: {e}")

    print(f"Logged in as {bot.user} ({bot.user.id})")


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    if not message.guild:
        return

    if not freechat_enabled_by_guild[message.guild.id]:
        await bot.process_commands(message)
        return

    if not is_allowed_channel(message.channel.id):
        await bot.process_commands(message)
        return

    if isinstance(message.author, discord.Member):
        if not user_has_required_role(message.author):
            await bot.process_commands(message)
            return

    content = clean_text(message.content)
    if not content:
        await bot.process_commands(message)
        return

    add_history(message.channel.id, "user", message.author.display_name, content)

    mentioned = bot.user in message.mentions if bot.user else False
    replied_to_bot = False

    if message.reference and message.reference.resolved:
        ref = message.reference.resolved
        if isinstance(ref, discord.Message) and ref.author.id == bot.user.id:
            replied_to_bot = True

    mode = None

    if mentioned or replied_to_bot:
        mode = "direct_reply"
    else:
        if should_trigger_from_bait(content):
            if random.random() < BAIT_TRIGGER_REPLY_CHANCE:
                mode = "spontaneous_reply"
        elif not on_cooldown(message.channel.id) and random.random() < SPONTANEOUS_REPLY_CHANCE:
            mode = "spontaneous_reply"

    if mode and not on_cooldown(message.channel.id):
        try:
            async with message.channel.typing():
                ai_text = await generate_ai_text(
                    channel_id=message.channel.id,
                    latest_context=content,
                    mode=mode,
                )
            if ai_text:
                add_history(message.channel.id, "assistant", bot.user.display_name, ai_text)
                mark_replied(message.channel.id)
                await send_chunked(message.channel, ai_text)
        except Exception as e:
            print(f"AI reply error: {e}")

    if (
        len(channel_history[message.channel.id]) >= 6
        and not on_cooldown(message.channel.id)
        and random.random() < SPONTANEOUS_START_CHANCE
    ):
        try:
            seed = pick_random_starter()
            async with message.channel.typing():
                ai_text = await generate_ai_text(
                    channel_id=message.channel.id,
                    latest_context=seed,
                    mode="spontaneous_start",
                )
            if ai_text:
                add_history(message.channel.id, "assistant", bot.user.display_name, ai_text)
                mark_replied(message.channel.id)
                await send_chunked(message.channel, ai_text)
        except Exception as e:
            print(f"AI spontaneous start error: {e}")

    await bot.process_commands(message)


@bot.tree.command(name="freechat", description="Turn free autonomous chatting on or off for this server.")
@app_commands.describe(enabled="true = on, false = off")
async def freechat(interaction: discord.Interaction, enabled: bool):
    if interaction.guild is None:
        await interaction.response.send_message("Server only command.", ephemeral=True)
        return

    if not interaction.user.guild_permissions.manage_guild:
        await interaction.response.send_message(
            "You need Manage Server to use this.",
            ephemeral=True,
        )
        return

    freechat_enabled_by_guild[interaction.guild.id] = enabled
    await interaction.response.send_message(
        f"Free chat is now {'enabled' if enabled else 'disabled'}.",
        ephemeral=True,
    )


@bot.tree.command(name="speak", description="Force the bot to say something in character.")
@app_commands.describe(prompt="What should the bot react to?")
async def speak(interaction: discord.Interaction, prompt: str):
    if interaction.guild is None:
        await interaction.response.send_message("Server only command.", ephemeral=True)
        return

    await interaction.response.defer()

    try:
        add_history(interaction.channel_id, "user", interaction.user.display_name, prompt)
        ai_text = await generate_ai_text(
            channel_id=interaction.channel_id,
            latest_context=prompt,
            mode="direct_reply",
        )

        if not ai_text:
            await interaction.followup.send("I had a thought and then it left.")
            return

        add_history(interaction.channel_id, "assistant", bot.user.display_name, ai_text)
        mark_replied(interaction.channel_id)
        await interaction.followup.send(ai_text)

    except Exception as e:
        print(f"/speak error: {e}")
        await interaction.followup.send("Something broke. Tragic.")


@bot.tree.command(name="resetmemory", description="Clear the bot's memory for this channel.")
async def resetmemory(interaction: discord.Interaction):
    if interaction.guild is None:
        await interaction.response.send_message("Server only command.", ephemeral=True)
        return

    if not interaction.user.guild_permissions.manage_messages:
        await interaction.response.send_message(
            "You need Manage Messages to use this.",
            ephemeral=True,
        )
        return

    channel_history[interaction.channel_id].clear()
    await interaction.response.send_message("Memory wiped for this channel.", ephemeral=True)


bot.run(DISCORD_TOKEN)
