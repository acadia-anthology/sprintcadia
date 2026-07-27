import asyncio
import os
from datetime import datetime, timedelta, timezone

import discord
from discord import app_commands
from discord.ext import commands, tasks
import psycopg2
import psycopg2.pool
import psycopg2.extras

# ===== EMBED BRANDING =====
SPRINT_COLOR = discord.Color.from_str("#4FD1C5")   # active sprint / info
RESULT_COLOR = discord.Color.from_str("#F2994A")   # sprint ended / results
ALERT_COLOR = discord.Color.from_str("#EB5757")    # errors, cancellations

# ===== ENVIRONMENT VARIABLES =====
TOKEN = os.getenv("DISCORD_TOKEN", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")

GOAL_TYPE_CHOICES = [
    app_commands.Choice(name="Pages", value="pages"),
    app_commands.Choice(name="Minutes", value="minutes"),
]

WEEKDAY_CHOICES = [
    app_commands.Choice(name="Monday", value=0),
    app_commands.Choice(name="Tuesday", value=1),
    app_commands.Choice(name="Wednesday", value=2),
    app_commands.Choice(name="Thursday", value=3),
    app_commands.Choice(name="Friday", value=4),
    app_commands.Choice(name="Saturday", value=5),
    app_commands.Choice(name="Sunday", value=6),
]
WEEKDAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ===== POSTGRES CONNECTION POOL =====
PG_POOL = None
if DATABASE_URL:
    try:
        PG_POOL = psycopg2.pool.SimpleConnectionPool(1, 5, dsn=DATABASE_URL)
    except Exception as error:
        print("Postgres connection pool setup failed:", error)
        PG_POOL = None


async def run_blocking(func, *args, **kwargs):
    """Runs a synchronous psycopg2 call off the asyncio event loop so one
    slow query doesn't freeze every other command in the bot."""
    return await asyncio.to_thread(func, *args, **kwargs)


def get_pg_connection():
    if PG_POOL is None:
        return None
    try:
        return PG_POOL.getconn()
    except Exception as error:
        print("Postgres getconn failed:", error)
        return None


def release_pg_connection(conn):
    if PG_POOL is not None and conn is not None:
        try:
            PG_POOL.putconn(conn)
        except Exception as error:
            print("Postgres putconn failed:", error)


def init_postgres_schema():
    if PG_POOL is None:
        print("Postgres disabled: DATABASE_URL not set.")
        return

    conn = get_pg_connection()
    if conn is None:
        return

    try:
        with conn.cursor() as cur:
            # Servers you've approved this bot for. Everything else gets left on join.
            cur.execute("""
                CREATE TABLE IF NOT EXISTS allowed_guilds (
                    guild_id   TEXT PRIMARY KEY,
                    guild_name TEXT DEFAULT '',
                    added_by   TEXT DEFAULT '',
                    added_at   TIMESTAMPTZ DEFAULT now()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sprints (
                    id               SERIAL PRIMARY KEY,
                    guild_id         TEXT NOT NULL,
                    channel_id       TEXT NOT NULL,
                    host_id          TEXT NOT NULL,
                    goal_type        TEXT NOT NULL DEFAULT 'pages',
                    duration_minutes INT NOT NULL,
                    status           TEXT NOT NULL DEFAULT 'active',
                    started_at       TIMESTAMPTZ DEFAULT now(),
                    ends_at          TIMESTAMPTZ NOT NULL
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sprints_channel_status ON sprints (channel_id, status)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sprint_participants (
                    sprint_id       INT NOT NULL REFERENCES sprints(id),
                    user_id         TEXT NOT NULL,
                    joined_at       TIMESTAMPTZ DEFAULT now(),
                    progress_amount NUMERIC DEFAULT 0,
                    reported        BOOLEAN DEFAULT FALSE,
                    PRIMARY KEY (sprint_id, user_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_stats (
                    guild_id           TEXT NOT NULL,
                    user_id            TEXT NOT NULL,
                    total_sprints      INT NOT NULL DEFAULT 0,
                    total_pages        NUMERIC NOT NULL DEFAULT 0,
                    total_minutes_read NUMERIC NOT NULL DEFAULT 0,
                    PRIMARY KEY (guild_id, user_id)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scheduled_sprints (
                    id                 SERIAL PRIMARY KEY,
                    guild_id           TEXT NOT NULL,
                    channel_id         TEXT NOT NULL,
                    day_of_week        INT NOT NULL,
                    time_utc           TEXT NOT NULL,
                    duration_minutes   INT NOT NULL DEFAULT 20,
                    goal_type          TEXT NOT NULL DEFAULT 'pages',
                    created_by         TEXT NOT NULL,
                    active             BOOLEAN NOT NULL DEFAULT TRUE,
                    last_triggered_date TEXT DEFAULT ''
                )
            """)
        conn.commit()
        print("Postgres schema ready.")
    except Exception as error:
        print("Postgres schema init failed:", error)
        conn.rollback()
    finally:
        release_pg_connection(conn)


# ===== ALLOWLIST HELPERS =====
def is_guild_allowed(guild_id: int) -> bool:
    if PG_POOL is None:
        return True  # no DB configured yet — don't lock yourself out during setup
    conn = get_pg_connection()
    if conn is None:
        return True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM allowed_guilds WHERE guild_id = %s", (str(guild_id),))
            return cur.fetchone() is not None
    except Exception as error:
        print("is_guild_allowed error:", error)
        return True
    finally:
        release_pg_connection(conn)


def add_allowed_guild(guild_id: int, guild_name: str, added_by: int):
    conn = get_pg_connection()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO allowed_guilds (guild_id, guild_name, added_by)
                VALUES (%s, %s, %s)
                ON CONFLICT (guild_id) DO UPDATE SET guild_name = EXCLUDED.guild_name
            """, (str(guild_id), guild_name, str(added_by)))
        conn.commit()
    except Exception as error:
        print("add_allowed_guild error:", error)
        conn.rollback()
    finally:
        release_pg_connection(conn)


def remove_allowed_guild(guild_id: int):
    conn = get_pg_connection()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM allowed_guilds WHERE guild_id = %s", (str(guild_id),))
        conn.commit()
    except Exception as error:
        print("remove_allowed_guild error:", error)
        conn.rollback()
    finally:
        release_pg_connection(conn)


def list_allowed_guilds() -> list[dict]:
    conn = get_pg_connection()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT guild_id, guild_name, added_at FROM allowed_guilds ORDER BY added_at")
            return list(cur.fetchall())
    except Exception as error:
        print("list_allowed_guilds error:", error)
        return []
    finally:
        release_pg_connection(conn)


# ===== SPRINT DB HELPERS =====
def db_create_sprint(guild_id: int, channel_id: int, host_id: int, goal_type: str, duration_minutes: int) -> int | None:
    conn = get_pg_connection()
    if conn is None:
        return None
    try:
        ends_at = now_utc() + timedelta(minutes=duration_minutes)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sprints (guild_id, channel_id, host_id, goal_type, duration_minutes, ends_at)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
            """, (str(guild_id), str(channel_id), str(host_id), goal_type, duration_minutes, ends_at))
            sprint_id = cur.fetchone()[0]
            cur.execute("""
                INSERT INTO sprint_participants (sprint_id, user_id) VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (sprint_id, str(host_id)))
        conn.commit()
        return sprint_id
    except Exception as error:
        print("db_create_sprint error:", error)
        conn.rollback()
        return None
    finally:
        release_pg_connection(conn)


def db_get_active_sprint(channel_id: int) -> dict | None:
    conn = get_pg_connection()
    if conn is None:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM sprints WHERE channel_id = %s AND status = 'active'
                ORDER BY started_at DESC LIMIT 1
            """, (str(channel_id),))
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception as error:
        print("db_get_active_sprint error:", error)
        return None
    finally:
        release_pg_connection(conn)


def db_get_sprint(sprint_id: int) -> dict | None:
    conn = get_pg_connection()
    if conn is None:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM sprints WHERE id = %s", (sprint_id,))
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception as error:
        print("db_get_sprint error:", error)
        return None
    finally:
        release_pg_connection(conn)


def db_join_sprint(sprint_id: int, user_id: int) -> bool:
    conn = get_pg_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sprint_participants (sprint_id, user_id) VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (sprint_id, str(user_id)))
            joined = cur.rowcount > 0
        conn.commit()
        return joined
    except Exception as error:
        print("db_join_sprint error:", error)
        conn.rollback()
        return False
    finally:
        release_pg_connection(conn)


def db_get_participants(sprint_id: int) -> list[dict]:
    conn = get_pg_connection()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT user_id, progress_amount, reported FROM sprint_participants
                WHERE sprint_id = %s ORDER BY progress_amount DESC
            """, (sprint_id,))
            return list(cur.fetchall())
    except Exception as error:
        print("db_get_participants error:", error)
        return []
    finally:
        release_pg_connection(conn)


def db_find_reportable_sprint(channel_id: int, user_id: int) -> dict | None:
    """Most recent sprint in this channel the user joined and hasn't reported to yet."""
    conn = get_pg_connection()
    if conn is None:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT s.* FROM sprints s
                JOIN sprint_participants p ON p.sprint_id = s.id
                WHERE s.channel_id = %s AND p.user_id = %s AND p.reported = FALSE
                ORDER BY s.started_at DESC LIMIT 1
            """, (str(channel_id), str(user_id)))
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception as error:
        print("db_find_reportable_sprint error:", error)
        return None
    finally:
        release_pg_connection(conn)


def db_log_progress(sprint_id: int, user_id: int, amount: float, goal_type: str, guild_id: int):
    conn = get_pg_connection()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE sprint_participants SET progress_amount = %s, reported = TRUE
                WHERE sprint_id = %s AND user_id = %s
            """, (amount, sprint_id, str(user_id)))

            pages_delta = amount if goal_type == "pages" else 0
            minutes_delta = amount if goal_type == "minutes" else 0
            cur.execute("""
                INSERT INTO user_stats (guild_id, user_id, total_sprints, total_pages, total_minutes_read)
                VALUES (%s, %s, 1, %s, %s)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET
                    total_sprints = user_stats.total_sprints + 1,
                    total_pages = user_stats.total_pages + EXCLUDED.total_pages,
                    total_minutes_read = user_stats.total_minutes_read + EXCLUDED.total_minutes_read
            """, (str(guild_id), str(user_id), pages_delta, minutes_delta))
        conn.commit()
    except Exception as error:
        print("db_log_progress error:", error)
        conn.rollback()
    finally:
        release_pg_connection(conn)


def db_end_sprint(sprint_id: int, status: str = "ended"):
    conn = get_pg_connection()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE sprints SET status = %s WHERE id = %s", (status, sprint_id))
        conn.commit()
    except Exception as error:
        print("db_end_sprint error:", error)
        conn.rollback()
    finally:
        release_pg_connection(conn)


def db_get_all_active_sprints() -> list[dict]:
    conn = get_pg_connection()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM sprints WHERE status = 'active'")
            return list(cur.fetchall())
    except Exception as error:
        print("db_get_all_active_sprints error:", error)
        return []
    finally:
        release_pg_connection(conn)


def db_get_user_stats(guild_id: int, user_id: int) -> dict:
    conn = get_pg_connection()
    if conn is None:
        return {"total_sprints": 0, "total_pages": 0, "total_minutes_read": 0}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT total_sprints, total_pages, total_minutes_read FROM user_stats
                WHERE guild_id = %s AND user_id = %s
            """, (str(guild_id), str(user_id)))
            row = cur.fetchone()
            return dict(row) if row else {"total_sprints": 0, "total_pages": 0, "total_minutes_read": 0}
    except Exception as error:
        print("db_get_user_stats error:", error)
        return {"total_sprints": 0, "total_pages": 0, "total_minutes_read": 0}
    finally:
        release_pg_connection(conn)


def db_get_leaderboard(guild_id: int, metric: str, limit: int = 10) -> list[dict]:
    column = "total_pages" if metric == "pages" else "total_sprints"
    conn = get_pg_connection()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT user_id, total_sprints, total_pages, total_minutes_read FROM user_stats
                WHERE guild_id = %s ORDER BY {column} DESC LIMIT %s
            """, (str(guild_id), limit))
            return list(cur.fetchall())
    except Exception as error:
        print("db_get_leaderboard error:", error)
        return []
    finally:
        release_pg_connection(conn)


# ===== SCHEDULED SPRINT DB HELPERS =====
def db_add_scheduled_sprint(guild_id: int, channel_id: int, day_of_week: int, time_utc: str,
                             duration_minutes: int, goal_type: str, created_by: int) -> int | None:
    conn = get_pg_connection()
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO scheduled_sprints (guild_id, channel_id, day_of_week, time_utc,
                    duration_minutes, goal_type, created_by)
                VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
            """, (str(guild_id), str(channel_id), day_of_week, time_utc, duration_minutes, goal_type, str(created_by)))
            new_id = cur.fetchone()[0]
        conn.commit()
        return new_id
    except Exception as error:
        print("db_add_scheduled_sprint error:", error)
        conn.rollback()
        return None
    finally:
        release_pg_connection(conn)


def db_list_scheduled_sprints(guild_id: int) -> list[dict]:
    conn = get_pg_connection()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM scheduled_sprints WHERE guild_id = %s AND active = TRUE
                ORDER BY day_of_week, time_utc
            """, (str(guild_id),))
            return list(cur.fetchall())
    except Exception as error:
        print("db_list_scheduled_sprints error:", error)
        return []
    finally:
        release_pg_connection(conn)


def db_remove_scheduled_sprint(schedule_id: int, guild_id: int) -> bool:
    conn = get_pg_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE scheduled_sprints SET active = FALSE WHERE id = %s AND guild_id = %s
            """, (schedule_id, str(guild_id)))
            removed = cur.rowcount > 0
        conn.commit()
        return removed
    except Exception as error:
        print("db_remove_scheduled_sprint error:", error)
        conn.rollback()
        return False
    finally:
        release_pg_connection(conn)


def db_get_due_scheduled_sprints(day_of_week: int, time_utc: str, today_str: str) -> list[dict]:
    conn = get_pg_connection()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM scheduled_sprints
                WHERE active = TRUE AND day_of_week = %s AND time_utc = %s
                    AND last_triggered_date IS DISTINCT FROM %s
            """, (day_of_week, time_utc, today_str))
            return list(cur.fetchall())
    except Exception as error:
        print("db_get_due_scheduled_sprints error:", error)
        return []
    finally:
        release_pg_connection(conn)


def db_mark_scheduled_triggered(schedule_id: int, today_str: str):
    conn = get_pg_connection()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE scheduled_sprints SET last_triggered_date = %s WHERE id = %s", (today_str, schedule_id))
        conn.commit()
    except Exception as error:
        print("db_mark_scheduled_triggered error:", error)
        conn.rollback()
    finally:
        release_pg_connection(conn)


# ===== EMBED BUILDERS =====
def build_sprint_start_embed(host: discord.Member, goal_type: str, duration_minutes: int, ends_at: datetime) -> discord.Embed:
    embed = discord.Embed(
        title="📖 Sprint started!",
        description=(
            f"{host.mention} kicked off a **{duration_minutes}-minute** reading sprint.\n"
            f"Goal: track your **{goal_type}** by the end.\n\n"
            f"Use `/sprint join` to jump in any time before it ends."
        ),
        color=SPRINT_COLOR,
    )
    embed.add_field(name="Ends", value=f"<t:{int(ends_at.timestamp())}:R>", inline=True)
    embed.set_footer(text="Sprintcadia")
    return embed


def build_sprint_status_embed(sprint: dict, participants: list[dict]) -> discord.Embed:
    ends_at = sprint["ends_at"]
    lines = [f"<@{p['user_id']}>" for p in participants] or ["No one has joined yet."]
    embed = discord.Embed(
        title="⏱️ Sprint status",
        description=f"Ends <t:{int(ends_at.timestamp())}:R>\nGoal type: **{sprint['goal_type']}**",
        color=SPRINT_COLOR,
    )
    embed.add_field(name=f"Participants ({len(participants)})", value="\n".join(lines), inline=False)
    embed.set_footer(text="Sprintcadia")
    return embed


def build_sprint_end_embed(sprint: dict) -> discord.Embed:
    embed = discord.Embed(
        title="⏰ Time's up!",
        description=f"The {sprint['duration_minutes']}-minute sprint has ended. Report your progress with `/sprint done`.",
        color=RESULT_COLOR,
    )
    embed.set_footer(text="Sprintcadia")
    return embed


def build_sprint_results_embed(sprint: dict, participants: list[dict]) -> discord.Embed:
    goal_type = sprint["goal_type"]
    reported = [p for p in participants if p["reported"]]
    unreported = [p for p in participants if not p["reported"]]

    total = sum(float(p["progress_amount"]) for p in reported)
    lines = [f"<@{p['user_id']}> — **{p['progress_amount']:g}** {goal_type}" for p in reported] or ["No one has reported yet."]

    embed = discord.Embed(title="🏁 Sprint results", color=RESULT_COLOR)
    embed.add_field(name="Reported", value="\n".join(lines), inline=False)
    if unreported:
        embed.add_field(
            name="Still waiting on",
            value="\n".join(f"<@{p['user_id']}>" for p in unreported),
            inline=False,
        )
    embed.add_field(name=f"Group total ({goal_type})", value=f"**{total:g}**", inline=False)
    embed.set_footer(text="Sprintcadia")
    return embed


def build_stats_embed(member: discord.abc.User, stats: dict) -> discord.Embed:
    embed = discord.Embed(title=f"📊 {member.display_name}'s stats", color=SPRINT_COLOR)
    embed.add_field(name="Sprints completed", value=str(stats["total_sprints"]), inline=True)
    embed.add_field(name="Total pages", value=f"{float(stats['total_pages']):g}", inline=True)
    embed.add_field(name="Total minutes", value=f"{float(stats['total_minutes_read']):g}", inline=True)
    embed.set_footer(text="Sprintcadia")
    return embed


def build_leaderboard_embed(guild_name: str, metric: str, rows: list[dict]) -> discord.Embed:
    embed = discord.Embed(title=f"🏆 {guild_name} leaderboard — {metric}", color=SPRINT_COLOR)
    if not rows:
        embed.description = "No sprint data yet."
        return embed
    medals = ["🥇", "🥈", "🥉"]
    lines = []
    for i, row in enumerate(rows):
        prefix = medals[i] if i < len(medals) else f"{i + 1}."
        value = row["total_pages"] if metric == "pages" else row["total_sprints"]
        unit = "pages" if metric == "pages" else "sprints"
        lines.append(f"{prefix} <@{row['user_id']}> — **{float(value):g}** {unit}")
    embed.description = "\n".join(lines)
    embed.set_footer(text="Sprintcadia")
    return embed


# ===== BOT SETUP =====
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

active_sprint_tasks: dict[int, asyncio.Task] = {}


async def finish_sprint(sprint_id: int):
    """Waits out the remaining sprint time, then posts results and closes it out."""
    sprint = await run_blocking(db_get_sprint, sprint_id)
    if sprint is None or sprint["status"] != "active":
        return

    remaining = (sprint["ends_at"] - now_utc()).total_seconds()
    if remaining > 0:
        await asyncio.sleep(remaining)

    sprint = await run_blocking(db_get_sprint, sprint_id)
    if sprint is None or sprint["status"] != "active":
        return
    await run_blocking(db_end_sprint, sprint_id, "ended")
    active_sprint_tasks.pop(sprint_id, None)

    channel = bot.get_channel(int(sprint["channel_id"]))
    if channel is None:
        return
    try:
        await channel.send(embed=build_sprint_end_embed(sprint))
    except discord.HTTPException as error:
        print("Failed to post sprint end embed:", error)


async def start_sprint(guild_id: int, channel: discord.abc.Messageable, host: discord.Member,
                        goal_type: str, duration_minutes: int) -> bool:
    existing = await run_blocking(db_get_active_sprint, channel.id)
    if existing is not None:
        return False

    sprint_id = await run_blocking(db_create_sprint, guild_id, channel.id, host.id, goal_type, duration_minutes)
    if sprint_id is None:
        return False

    sprint = await run_blocking(db_get_sprint, sprint_id)
    await channel.send(embed=build_sprint_start_embed(host, goal_type, duration_minutes, sprint["ends_at"]))
    active_sprint_tasks[sprint_id] = bot.loop.create_task(finish_sprint(sprint_id))
    return True


# ===== /sprint COMMANDS =====
sprint_group = app_commands.Group(name="sprint", description="Run a reading sprint in this channel.")


@sprint_group.command(name="start", description="Start a reading sprint in this channel.")
@app_commands.describe(duration_minutes="How long the sprint runs, in minutes", goal_type="What participants will report at the end")
@app_commands.choices(goal_type=GOAL_TYPE_CHOICES)
async def sprint_start(interaction: discord.Interaction, duration_minutes: app_commands.Range[int, 1, 180],
                        goal_type: app_commands.Choice[str]):
    started = await start_sprint(interaction.guild_id, interaction.channel, interaction.user, goal_type.value, duration_minutes)
    if not started:
        await interaction.response.send_message(
            embed=discord.Embed(description="⚠️ There's already an active sprint in this channel.", color=ALERT_COLOR),
            ephemeral=True,
        )
        return
    await interaction.response.send_message("Sprint started!", ephemeral=True)


@sprint_group.command(name="join", description="Join the active sprint in this channel.")
async def sprint_join(interaction: discord.Interaction):
    sprint = await run_blocking(db_get_active_sprint, interaction.channel_id)
    if sprint is None:
        await interaction.response.send_message(
            embed=discord.Embed(description="No active sprint here. Start one with `/sprint start`.", color=ALERT_COLOR),
            ephemeral=True,
        )
        return
    joined = await run_blocking(db_join_sprint, sprint["id"], interaction.user.id)
    if joined:
        await interaction.response.send_message(f"✅ {interaction.user.mention} joined the sprint!")
    else:
        await interaction.response.send_message("You're already in this sprint.", ephemeral=True)


@sprint_group.command(name="status", description="Show the active sprint's time remaining and participants.")
async def sprint_status(interaction: discord.Interaction):
    sprint = await run_blocking(db_get_active_sprint, interaction.channel_id)
    if sprint is None:
        await interaction.response.send_message(
            embed=discord.Embed(description="No active sprint here.", color=ALERT_COLOR), ephemeral=True,
        )
        return
    participants = await run_blocking(db_get_participants, sprint["id"])
    await interaction.response.send_message(embed=build_sprint_status_embed(sprint, participants))


@sprint_group.command(name="done", description="Report your progress for the sprint you joined.")
@app_commands.describe(amount="How many pages/minutes you got through")
async def sprint_done(interaction: discord.Interaction, amount: app_commands.Range[float, 0, None]):
    sprint = await run_blocking(db_find_reportable_sprint, interaction.channel_id, interaction.user.id)
    if sprint is None:
        await interaction.response.send_message(
            embed=discord.Embed(description="No sprint here for you to report progress on.", color=ALERT_COLOR),
            ephemeral=True,
        )
        return
    await run_blocking(db_log_progress, sprint["id"], interaction.user.id, amount, sprint["goal_type"], interaction.guild_id)
    await interaction.response.send_message(f"📗 Logged **{amount:g} {sprint['goal_type']}** for {interaction.user.mention}.")


@sprint_group.command(name="results", description="Show results for the most recent sprint in this channel.")
async def sprint_results(interaction: discord.Interaction):
    sprint = await run_blocking(db_get_active_sprint, interaction.channel_id)
    if sprint is None:
        sprint = await run_blocking(db_find_reportable_sprint, interaction.channel_id, interaction.user.id)
    if sprint is None:
        await interaction.response.send_message(
            embed=discord.Embed(description="No recent sprint found in this channel.", color=ALERT_COLOR),
            ephemeral=True,
        )
        return
    participants = await run_blocking(db_get_participants, sprint["id"])
    await interaction.response.send_message(embed=build_sprint_results_embed(sprint, participants))


@sprint_group.command(name="cancel", description="Cancel the active sprint in this channel.")
async def sprint_cancel(interaction: discord.Interaction):
    sprint = await run_blocking(db_get_active_sprint, interaction.channel_id)
    if sprint is None:
        await interaction.response.send_message(
            embed=discord.Embed(description="No active sprint here.", color=ALERT_COLOR), ephemeral=True,
        )
        return

    is_host = str(interaction.user.id) == sprint["host_id"]
    is_admin = isinstance(interaction.user, discord.Member) and interaction.user.guild_permissions.administrator
    if not (is_host or is_admin):
        await interaction.response.send_message(
            embed=discord.Embed(description="Only the host or a server admin can cancel this sprint.", color=ALERT_COLOR),
            ephemeral=True,
        )
        return

    await run_blocking(db_end_sprint, sprint["id"], "cancelled")
    task = active_sprint_tasks.pop(sprint["id"], None)
    if task is not None:
        task.cancel()
    await interaction.response.send_message(
        embed=discord.Embed(description="🛑 Sprint cancelled.", color=ALERT_COLOR),
    )


bot.tree.add_command(sprint_group)


# ===== STATS / LEADERBOARD COMMANDS =====
@bot.tree.command(name="stats", description="Show your (or someone else's) all-time sprint stats.")
@app_commands.describe(member="Whose stats to show (defaults to you)")
async def stats_command(interaction: discord.Interaction, member: discord.Member | None = None):
    target = member or interaction.user
    stats = await run_blocking(db_get_user_stats, interaction.guild_id, target.id)
    await interaction.response.send_message(embed=build_stats_embed(target, stats))


@bot.tree.command(name="leaderboard", description="Show this server's sprint leaderboard.")
@app_commands.describe(metric="Rank by total pages read or total sprints completed")
@app_commands.choices(metric=[
    app_commands.Choice(name="Total pages", value="pages"),
    app_commands.Choice(name="Sprints completed", value="sprints"),
])
async def leaderboard_command(interaction: discord.Interaction, metric: app_commands.Choice[str] = None):
    metric_value = metric.value if metric else "pages"
    rows = await run_blocking(db_get_leaderboard, interaction.guild_id, metric_value)
    await interaction.response.send_message(embed=build_leaderboard_embed(interaction.guild.name, metric_value, rows))


# ===== /schedule COMMANDS (Admin only) =====
schedule_group = app_commands.Group(name="schedule", description="Manage recurring scheduled sprints. Admin only.")


@schedule_group.command(name="add", description="Schedule a recurring sprint in a channel.")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(
    channel="Channel the sprint will post in",
    day="Day of week (UTC)",
    time_utc="Time in 24h UTC, e.g. 18:00",
    duration_minutes="Sprint length in minutes",
    goal_type="What participants report at the end",
)
@app_commands.choices(day=WEEKDAY_CHOICES, goal_type=GOAL_TYPE_CHOICES)
async def schedule_add(interaction: discord.Interaction, channel: discord.TextChannel, day: app_commands.Choice[int],
                        time_utc: str, duration_minutes: app_commands.Range[int, 1, 180], goal_type: app_commands.Choice[str]):
    if not _valid_time_str(time_utc):
        await interaction.response.send_message(
            embed=discord.Embed(description="⚠️ Time must be in 24h `HH:MM` format, e.g. `18:00`.", color=ALERT_COLOR),
            ephemeral=True,
        )
        return
    await run_blocking(db_add_scheduled_sprint, interaction.guild_id, channel.id, day.value, time_utc,
                       duration_minutes, goal_type.value, interaction.user.id)
    await interaction.response.send_message(
        f"✅ Scheduled a {duration_minutes}-minute sprint in {channel.mention} every "
        f"**{WEEKDAY_NAMES[day.value]} at {time_utc} UTC**."
    )


@schedule_group.command(name="list", description="List this server's scheduled sprints.")
@app_commands.checks.has_permissions(administrator=True)
async def schedule_list(interaction: discord.Interaction):
    rows = await run_blocking(db_list_scheduled_sprints, interaction.guild_id)
    if not rows:
        await interaction.response.send_message("No scheduled sprints set up yet.", ephemeral=True)
        return
    lines = [
        f"`#{r['id']}` <#{r['channel_id']}> — {WEEKDAY_NAMES[r['day_of_week']]} {r['time_utc']} UTC, "
        f"{r['duration_minutes']}min, {r['goal_type']}"
        for r in rows
    ]
    embed = discord.Embed(title="📅 Scheduled sprints", description="\n".join(lines), color=SPRINT_COLOR)
    await interaction.response.send_message(embed=embed)


@schedule_group.command(name="remove", description="Remove a scheduled sprint by its ID.")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(schedule_id="ID shown in /schedule list")
async def schedule_remove(interaction: discord.Interaction, schedule_id: int):
    removed = await run_blocking(db_remove_scheduled_sprint, schedule_id, interaction.guild_id)
    if removed:
        await interaction.response.send_message(f"🗑️ Removed schedule `#{schedule_id}`.")
    else:
        await interaction.response.send_message(
            embed=discord.Embed(description="No schedule with that ID in this server.", color=ALERT_COLOR),
            ephemeral=True,
        )


def _valid_time_str(value: str) -> bool:
    try:
        hh, mm = value.split(":")
        return 0 <= int(hh) <= 23 and 0 <= int(mm) <= 59
    except Exception:
        return False


bot.tree.add_command(schedule_group)


# ===== /bot COMMANDS (Bot owner only — controls the allowlist) =====
bot_group = app_commands.Group(name="bot", description="Bot owner only: manage which servers Sprintcadia stays in.")


@bot_group.command(name="allowlist-add", description="Owner only: approve a server for Sprintcadia.")
@app_commands.describe(guild_id="The server ID to approve")
async def bot_allowlist_add(interaction: discord.Interaction, guild_id: str):
    if not await bot.is_owner(interaction.user):
        await interaction.response.send_message("🔒 Bot owner only.", ephemeral=True)
        return
    guild = bot.get_guild(int(guild_id))
    guild_name = guild.name if guild else ""
    await run_blocking(add_allowed_guild, int(guild_id), guild_name, interaction.user.id)
    await interaction.response.send_message(f"✅ Approved guild `{guild_id}`" + (f" ({guild_name})" if guild_name else ""), ephemeral=True)


@bot_group.command(name="allowlist-remove", description="Owner only: revoke a server's approval. Bot leaves if currently a member.")
@app_commands.describe(guild_id="The server ID to revoke")
async def bot_allowlist_remove(interaction: discord.Interaction, guild_id: str):
    if not await bot.is_owner(interaction.user):
        await interaction.response.send_message("🔒 Bot owner only.", ephemeral=True)
        return
    await run_blocking(remove_allowed_guild, int(guild_id))
    guild = bot.get_guild(int(guild_id))
    if guild is not None:
        await guild.leave()
    await interaction.response.send_message(f"🚫 Revoked guild `{guild_id}`" + (" and left it." if guild else "."), ephemeral=True)


@bot_group.command(name="allowlist-view", description="Owner only: list approved servers.")
async def bot_allowlist_view(interaction: discord.Interaction):
    if not await bot.is_owner(interaction.user):
        await interaction.response.send_message("🔒 Bot owner only.", ephemeral=True)
        return
    rows = await run_blocking(list_allowed_guilds)
    if not rows:
        await interaction.response.send_message("No servers approved yet.", ephemeral=True)
        return
    lines = [f"`{r['guild_id']}` — {r['guild_name'] or '(unknown name)'}" for r in rows]
    await interaction.response.send_message("\n".join(lines), ephemeral=True)


bot.tree.add_command(bot_group)


# ===== SCHEDULED SPRINT BACKGROUND LOOP =====
@tasks.loop(minutes=1)
async def scheduled_sprint_loop():
    now = now_utc()
    time_str = now.strftime("%H:%M")
    today_str = now.strftime("%Y-%m-%d")
    due = await run_blocking(db_get_due_scheduled_sprints, now.weekday(), time_str, today_str)

    for row in due:
        await run_blocking(db_mark_scheduled_triggered, row["id"], today_str)
        channel = bot.get_channel(int(row["channel_id"]))
        guild = bot.get_guild(int(row["guild_id"]))
        if channel is None or guild is None:
            continue
        host = guild.me
        try:
            await start_sprint(int(row["guild_id"]), channel, host, row["goal_type"], row["duration_minutes"])
        except Exception as error:
            print("scheduled_sprint_loop start_sprint error:", error)


# ===== ALLOWLIST ENFORCEMENT =====
@bot.event
async def on_guild_join(guild: discord.Guild):
    if not is_guild_allowed(guild.id):
        print(f"Leaving unapproved guild: {guild.name} ({guild.id})")
        try:
            if guild.system_channel:
                await guild.system_channel.send(
                    "This bot is invite-only right now and this server hasn't been approved. Leaving!"
                )
        except discord.HTTPException:
            pass
        await guild.leave()
        return
    try:
        bot.tree.copy_global_to(guild=guild)
        await bot.tree.sync(guild=guild)
    except Exception as error:
        print(f"Command sync failed for {guild.name}:", error)


# ===== GLOBAL SLASH COMMAND ERROR HANDLER =====
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    print("Unhandled slash command error:", repr(error))
    if isinstance(error, app_commands.errors.MissingPermissions):
        message = "🔒 Admin only."
    else:
        message = f"❌ Something went wrong running that command.\n`{error}`"
    try:
        if interaction.response.is_done():
            await interaction.followup.send(message, ephemeral=True)
        else:
            await interaction.response.send_message(message, ephemeral=True)
    except discord.HTTPException:
        pass


# ===== BOT READY =====
@bot.event
async def on_ready():
    print(f"{bot.user} is online!")

    await run_blocking(init_postgres_schema)

    # Leave anything already joined that isn't on the allowlist (e.g. removed while offline).
    for guild in list(bot.guilds):
        if not await run_blocking(is_guild_allowed, guild.id):
            print(f"Leaving unapproved guild found on startup: {guild.name} ({guild.id})")
            await guild.leave()

    # Re-arm timers for sprints that were still running when the bot last restarted.
    active_sprints = await run_blocking(db_get_all_active_sprints)
    for sprint in active_sprints:
        active_sprint_tasks[sprint["id"]] = bot.loop.create_task(finish_sprint(sprint["id"]))
    if active_sprints:
        print(f"Re-armed {len(active_sprints)} in-progress sprint(s).")

    if not scheduled_sprint_loop.is_running():
        scheduled_sprint_loop.start()

    for guild in bot.guilds:
        try:
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
            print(f"Synced {len(synced)} command(s) to {guild.name}.")
        except Exception as error:
            print(f"Guild slash command sync failed for {guild.name}:", error)


# ===== START BOT =====
bot.run(TOKEN)
