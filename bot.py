import discord
from discord.ext import commands
from core.config import load_config
from core import db

cfg = load_config()
TOKEN = (cfg.get("token") or "").strip()

ADMIN_SERVER_ID = int(cfg.get("admin_server_id") or 0)

intents = discord.Intents.default()
intents.guilds = True
intents.members = True
intents.reactions = True
intents.messages = True

bot = commands.Bot(command_prefix=commands.when_mentioned, intents=intents)


@bot.event
async def setup_hook():
    db.init()
    db.prune_rep()

    extensions = [
        "cogs.channels",
        "cogs.verification",
        "cogs.wordfilter",   # ? broadcast-only word filter helper
        "cogs.broadcasts",
        "cogs.reputation",
        "cogs.feedback",
        "cogs.admin",
        "cogs.groups",
	"cogs.help",
    ]

    loaded = []
    failed = []

    # Load feature cogs
    for ext in extensions:
        try:
            await bot.load_extension(ext)
            loaded.append(ext)
        except Exception as e:
            failed.append((ext, repr(e)))

    # Print startup summary
    print("?? Extension load summary:")
    for ext in loaded:
        print(f"  ? {ext}")
    for ext, err in failed:
        print(f"  ? {ext} -> {err}")

    # Sync globals + admin guild commands
    await bot.tree.sync()
    await bot.tree.sync(guild=discord.Object(id=ADMIN_SERVER_ID))


@bot.event
async def on_ready():
    print("? Bot Online")
    print(f"?? FoxCom online as {bot.user}")


if not TOKEN:
    print("? Put token in config.json")
else:
    bot.run(TOKEN)
