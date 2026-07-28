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

# Fanfic word-count-to-page conversion, matching Abraxos's "Fanfic Words Per Damage" default.
FANFIC_WORDS_PER_PAGE = 250

SPRINT_ROLE_SETTING_KEY = "Sprint Ping Role ID"

# ===== CUSTOM APPLICATION EMOJIS (uploaded to the bot itself in the Dev Portal) =====
EMOJI_TIMER = "<:timer:1531488929201524958>"
EMOJI_SPRINTING = "<:sprinting:1531488927750422578>"
EMOJI_QUILL = "<:quill:1531488926831738890>"
EMOJI_PIN = "<:pin:1531488925686693888>"
EMOJI_PHOENIXICON = "<:phoenixicon:1531488924961214515>"
EMOJI_PHOENIX = "<:phoenix:1531488924113830023>"
EMOJI_PERCENTAGE = "<:percentage:1531488922650022019>"
EMOJI_PAGE = "<:page:1531488921782063134>"
EMOJI_OPENBOOK = "<:openbook:1531488920393486426>"
EMOJI_LOG = "<:log:1531488919600890017>"
EMOJI_HOURGLASS = "<:hourglass:1531488918674079804>"
EMOJI_EBOOK = "<:ebook:1531488917545681027>"
EMOJI_BOOKSTACK = "<:bookstack:1531488916685983774>"
EMOJI_BOOK = "<:book:1531488915876352070>"
EMOJI_AUDIOBOOK = "<:audiobook:1531488915100401735>"
EMOJI_FIRE = "<:fire:1531488913871474809>"

PHOENIX_FOOTER_ICON_URL = "https://cdn.discordapp.com/emojis/1531488924961214515.png"


def _set_footer(embed: discord.Embed):
    embed.set_footer(text="Sprintcadia", icon_url=PHOENIX_FOOTER_ICON_URL)


LOG_TYPE_ICONS = {"pages": EMOJI_PAGE, "percentage": EMOJI_PERCENTAGE, "audio": EMOJI_AUDIOBOOK, "fanfic": EMOJI_QUILL}
LOG_TYPE_LABELS = {"pages": "Pages", "percentage": "Ebook %", "audio": "Audiobook", "fanfic": "Fanfic"}

JOIN_LOG_TYPE_CHOICES = [
    app_commands.Choice(name="Pages", value="pages"),
    app_commands.Choice(name="Ebook %", value="percentage"),
    app_commands.Choice(name="Audiobook", value="audio"),
    app_commands.Choice(name="Fanfic", value="fanfic"),
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


def _parse_int(text: str) -> int | None:
    try:
        return int(str(text).strip())
    except (TypeError, ValueError):
        return None


def _parse_float(text: str) -> float | None:
    try:
        return float(str(text).strip())
    except (TypeError, ValueError):
        return None


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
            # Generic per-server settings (e.g. which role to ping when a sprint starts).
            cur.execute("""
                CREATE TABLE IF NOT EXISTS guild_settings (
                    guild_id TEXT NOT NULL,
                    key      TEXT NOT NULL,
                    value    TEXT DEFAULT '',
                    PRIMARY KEY (guild_id, key)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sprints (
                    id                       SERIAL PRIMARY KEY,
                    guild_id                 TEXT NOT NULL,
                    channel_id               TEXT NOT NULL,
                    host_id                  TEXT NOT NULL,
                    duration_minutes         INT NOT NULL,
                    status                   TEXT NOT NULL DEFAULT 'active',
                    started_at               TIMESTAMPTZ DEFAULT now(),
                    ends_at                  TIMESTAMPTZ NOT NULL,
                    announcement_message_id  TEXT DEFAULT ''
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sprints_channel_status ON sprints (channel_id, status)")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sprint_participants (
                    sprint_id          INT NOT NULL REFERENCES sprints(id),
                    user_id            TEXT NOT NULL,
                    joined_at          TIMESTAMPTZ DEFAULT now(),
                    log_type           TEXT DEFAULT '',
                    book_title         TEXT DEFAULT '',
                    raw_amount         NUMERIC,
                    pages_equivalent   NUMERIC,
                    minutes_equivalent NUMERIC,
                    reported           BOOLEAN DEFAULT FALSE,
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
                    id                  SERIAL PRIMARY KEY,
                    guild_id            TEXT NOT NULL,
                    channel_id          TEXT NOT NULL,
                    day_of_week         INT NOT NULL,
                    time_utc            TEXT NOT NULL,
                    duration_minutes    INT NOT NULL DEFAULT 20,
                    created_by          TEXT NOT NULL,
                    active              BOOLEAN NOT NULL DEFAULT TRUE,
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


# ===== GUILD SETTINGS HELPERS =====
def get_guild_setting(guild_id: int, key: str, default: str = "") -> str:
    conn = get_pg_connection()
    if conn is None:
        return default
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM guild_settings WHERE guild_id = %s AND key = %s", (str(guild_id), key))
            row = cur.fetchone()
            return row[0] if row else default
    except Exception as error:
        print("get_guild_setting error:", error)
        return default
    finally:
        release_pg_connection(conn)


def set_guild_setting(guild_id: int, key: str, value: str):
    conn = get_pg_connection()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO guild_settings (guild_id, key, value) VALUES (%s, %s, %s)
                ON CONFLICT (guild_id, key) DO UPDATE SET value = EXCLUDED.value
            """, (str(guild_id), key, value))
        conn.commit()
    except Exception as error:
        print("set_guild_setting error:", error)
        conn.rollback()
    finally:
        release_pg_connection(conn)


# ===== SPRINT DB HELPERS =====
def db_create_sprint(guild_id: int, channel_id: int, host_id: int, duration_minutes: int) -> int | None:
    conn = get_pg_connection()
    if conn is None:
        return None
    try:
        ends_at = now_utc() + timedelta(minutes=duration_minutes)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sprints (guild_id, channel_id, host_id, duration_minutes, ends_at)
                VALUES (%s, %s, %s, %s, %s) RETURNING id
            """, (str(guild_id), str(channel_id), str(host_id), duration_minutes, ends_at))
            sprint_id = cur.fetchone()[0]
        conn.commit()
        return sprint_id
    except Exception as error:
        print("db_create_sprint error:", error)
        conn.rollback()
        return None
    finally:
        release_pg_connection(conn)


def db_set_sprint_message(sprint_id: int, message_id: int):
    conn = get_pg_connection()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE sprints SET announcement_message_id = %s WHERE id = %s", (str(message_id), sprint_id))
        conn.commit()
    except Exception as error:
        print("db_set_sprint_message error:", error)
        conn.rollback()
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


def db_join_sprint(sprint_id: int, user_id: int, log_type: str, book_title: str = "") -> bool:
    conn = get_pg_connection()
    if conn is None:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sprint_participants (sprint_id, user_id, log_type, book_title)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (sprint_id, str(user_id), log_type, book_title))
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
                SELECT user_id, log_type, book_title, raw_amount, pages_equivalent, minutes_equivalent, reported
                FROM sprint_participants
                WHERE sprint_id = %s
                ORDER BY reported DESC, COALESCE(pages_equivalent, 0) DESC, joined_at ASC
            """, (sprint_id,))
            return list(cur.fetchall())
    except Exception as error:
        print("db_get_participants error:", error)
        return []
    finally:
        release_pg_connection(conn)


def db_find_loggable_sprint(channel_id: int, user_id: int) -> dict | None:
    """The sprint in this channel the user should log progress against: the active one
    if they're in it, otherwise the most recent one they joined. Includes their own
    log_type/book_title from that sprint so the log button knows which modal to open."""
    conn = get_pg_connection()
    if conn is None:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT s.*, p.log_type AS participant_log_type, p.book_title AS participant_book_title
                FROM sprints s
                JOIN sprint_participants p ON p.sprint_id = s.id
                WHERE s.channel_id = %s AND p.user_id = %s
                ORDER BY (s.status = 'active') DESC, s.started_at DESC LIMIT 1
            """, (str(channel_id), str(user_id)))
            row = cur.fetchone()
            return dict(row) if row else None
    except Exception as error:
        print("db_find_loggable_sprint error:", error)
        return None
    finally:
        release_pg_connection(conn)


def db_log_progress(sprint_id: int, user_id: int, guild_id: int, log_type: str,
                     raw_amount: float, pages_equivalent: float | None, minutes_equivalent: float | None):
    """Logging is repeatable (like an 'update progress' action) — each call overwrites the
    participant's prior report and only nudges user_stats by the *difference*, so re-logging
    doesn't double-count pages/minutes already counted from an earlier report this sprint."""
    conn = get_pg_connection()
    if conn is None:
        return
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT pages_equivalent, minutes_equivalent, reported FROM sprint_participants
                WHERE sprint_id = %s AND user_id = %s FOR UPDATE
            """, (sprint_id, str(user_id)))
            row = cur.fetchone()
            old_pages = float(row["pages_equivalent"]) if row and row["pages_equivalent"] is not None else 0.0
            old_minutes = float(row["minutes_equivalent"]) if row and row["minutes_equivalent"] is not None else 0.0
            was_reported = bool(row["reported"]) if row else False

            cur.execute("""
                UPDATE sprint_participants
                SET log_type = %s, raw_amount = %s, pages_equivalent = %s, minutes_equivalent = %s, reported = TRUE
                WHERE sprint_id = %s AND user_id = %s
            """, (log_type, raw_amount, pages_equivalent, minutes_equivalent, sprint_id, str(user_id)))

            pages_delta = (pages_equivalent or 0) - old_pages
            minutes_delta = (minutes_equivalent or 0) - old_minutes
            sprint_delta = 0 if was_reported else 1

            cur.execute("""
                INSERT INTO user_stats (guild_id, user_id, total_sprints, total_pages, total_minutes_read)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (guild_id, user_id) DO UPDATE SET
                    total_sprints = user_stats.total_sprints + EXCLUDED.total_sprints,
                    total_pages = user_stats.total_pages + EXCLUDED.total_pages,
                    total_minutes_read = user_stats.total_minutes_read + EXCLUDED.total_minutes_read
            """, (str(guild_id), str(user_id), sprint_delta, pages_delta, minutes_delta))
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
                             duration_minutes: int, created_by: int) -> int | None:
    conn = get_pg_connection()
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO scheduled_sprints (guild_id, channel_id, day_of_week, time_utc,
                    duration_minutes, created_by)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
            """, (str(guild_id), str(channel_id), day_of_week, time_utc, duration_minutes, str(created_by)))
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
def _participant_line(p: dict) -> str:
    icon = LOG_TYPE_ICONS.get(p.get("log_type"), EMOJI_OPENBOOK)
    title = f" — reading *{p['book_title']}*" if p.get("book_title") else ""
    return f"{icon} <@{p['user_id']}>{title}"


def build_sprint_announcement_embed(sprint: dict, participants: list[dict]) -> discord.Embed:
    names = "\n".join(_participant_line(p) for p in participants) or "No one yet — be the first!"
    embed = discord.Embed(title=f"{EMOJI_FIRE} Sprint ignited!", color=SPRINT_COLOR)
    embed.add_field(name=f"{EMOJI_TIMER} Duration", value=f"{sprint['duration_minutes']} min", inline=True)
    embed.add_field(name=f"{EMOJI_HOURGLASS} Starts", value=f"<t:{int(sprint['started_at'].timestamp())}:R>", inline=True)
    embed.add_field(name=f"{EMOJI_HOURGLASS} Ends", value=f"<t:{int(sprint['ends_at'].timestamp())}:R>", inline=True)
    embed.add_field(name=f"{EMOJI_BOOKSTACK} Participants ({len(participants)})", value=names, inline=False)
    embed.add_field(name=f"{EMOJI_PHOENIXICON} Host", value=f"<@{sprint['host_id']}>", inline=False)
    _set_footer(embed)
    return embed


def build_sprint_status_embed(sprint: dict, participants: list[dict]) -> discord.Embed:
    lines = [_participant_line(p) for p in participants] or ["No one has joined yet."]
    embed = discord.Embed(
        title=f"{EMOJI_TIMER} Sprint status",
        description=f"Ends <t:{int(sprint['ends_at'].timestamp())}:R>",
        color=SPRINT_COLOR,
    )
    embed.add_field(name=f"{EMOJI_BOOKSTACK} Participants ({len(participants)})", value="\n".join(lines), inline=False)
    _set_footer(embed)
    return embed


def build_sprint_end_embed(sprint: dict, participants: list[dict]) -> discord.Embed:
    names = "\n".join(_participant_line(p) for p in participants) or "No one joined this one."
    embed = discord.Embed(
        title=f"{EMOJI_PHOENIX} Sprint complete — rise & report!",
        description=f"The {sprint['duration_minutes']}-minute sprint is over. Tap **Log Sprint** below.",
        color=RESULT_COLOR,
    )
    embed.add_field(name=f"{EMOJI_BOOKSTACK} Participants", value=names, inline=False)
    _set_footer(embed)
    return embed


def _format_result_line(p: dict) -> str:
    uid = p["user_id"]
    title = f" *({p['book_title']})*" if p.get("book_title") else ""
    if not p["reported"]:
        return f"<@{uid}>{title} — no report yet"

    log_type = p["log_type"]
    raw = p["raw_amount"]
    if log_type == "pages":
        return f"<@{uid}>{title} — **{float(raw):g} pages**"
    if log_type == "percentage":
        return f"<@{uid}>{title} — **{float(raw):g}%** → **{float(p['pages_equivalent']):g} pages**"
    if log_type == "audio":
        return f"<@{uid}>{title} — **{float(raw):g}%** listened → **{float(p['minutes_equivalent']):g} min**"
    if log_type == "fanfic":
        return f"<@{uid}>{title} — **{float(raw):g} words** → **{float(p['pages_equivalent']):g} pages**"
    return f"<@{uid}>{title} — reported"


def build_sprint_results_embed(participants: list[dict]) -> discord.Embed:
    reported = [p for p in participants if p["reported"]]
    unreported = [p for p in participants if not p["reported"]]

    total_pages = sum(float(p["pages_equivalent"]) for p in reported if p["pages_equivalent"] is not None)
    total_minutes = sum(float(p["minutes_equivalent"]) for p in reported if p["minutes_equivalent"] is not None)

    embed = discord.Embed(title=f"{EMOJI_PHOENIXICON} Sprint results", color=RESULT_COLOR)
    embed.add_field(
        name="Reported",
        value="\n".join(_format_result_line(p) for p in reported) or "No one has reported yet.",
        inline=False,
    )
    if unreported:
        embed.add_field(
            name="Still waiting on",
            value="\n".join(f"<@{p['user_id']}>" for p in unreported),
            inline=False,
        )

    totals = []
    if total_pages:
        totals.append(f"**{total_pages:g} pages**")
    if total_minutes:
        totals.append(f"**{total_minutes:g} minutes** of audio")
    embed.add_field(name=f"{EMOJI_FIRE} Group total", value=" and ".join(totals) if totals else "No progress logged yet.", inline=False)
    _set_footer(embed)
    return embed


def build_stats_embed(member: discord.abc.User, stats: dict) -> discord.Embed:
    embed = discord.Embed(title=f"{EMOJI_PHOENIXICON} {member.display_name}'s stats", color=SPRINT_COLOR)
    embed.add_field(name=f"{EMOJI_SPRINTING} Sprints completed", value=str(stats["total_sprints"]), inline=True)
    embed.add_field(name=f"{EMOJI_PAGE} Total pages", value=f"{float(stats['total_pages']):g}", inline=True)
    embed.add_field(name=f"{EMOJI_AUDIOBOOK} Total minutes", value=f"{float(stats['total_minutes_read']):g}", inline=True)
    _set_footer(embed)
    return embed


def build_leaderboard_embed(guild_name: str, metric: str, rows: list[dict]) -> discord.Embed:
    embed = discord.Embed(title=f"{EMOJI_FIRE} {guild_name} leaderboard — {metric}", color=SPRINT_COLOR)
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
    _set_footer(embed)
    return embed


# ===== BOT SETUP =====
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

active_sprint_tasks: dict[int, asyncio.Task] = {}


async def apply_log(interaction: discord.Interaction, log_type: str, raw_amount: float,
                     pages_equivalent: float | None, minutes_equivalent: float | None, confirm_text: str):
    sprint = await run_blocking(db_find_loggable_sprint, interaction.channel_id, interaction.user.id)
    if sprint is None:
        embed = discord.Embed(
            description="You haven't joined a sprint here yet — use `/sprint join` or the Join button first.",
            color=ALERT_COLOR,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)
        return
    await run_blocking(db_log_progress, sprint["id"], interaction.user.id, interaction.guild_id,
                        log_type, raw_amount, pages_equivalent, minutes_equivalent)
    await interaction.response.send_message(confirm_text, ephemeral=True)


async def handle_join(interaction: discord.Interaction, log_type: str, book_title: str = ""):
    sprint = await run_blocking(db_get_active_sprint, interaction.channel_id)
    if sprint is None:
        await interaction.response.send_message(
            embed=discord.Embed(description="No active sprint here. Start one with `/sprint start`.", color=ALERT_COLOR),
            ephemeral=True,
        )
        return
    joined = await run_blocking(db_join_sprint, sprint["id"], interaction.user.id, log_type, book_title.strip())
    if joined:
        label = LOG_TYPE_LABELS.get(log_type, log_type)
        extra = f" — reading *{book_title.strip()}*" if book_title.strip() else ""
        await interaction.response.send_message(f"✅ {interaction.user.mention} joined, tracking **{label}**{extra}!")
        await refresh_sprint_announcement(sprint["id"])
    else:
        await interaction.response.send_message("You're already in this sprint.", ephemeral=True)


async def open_log_modal_for_user(interaction: discord.Interaction):
    """Looks up how this user is tracking their current/most-recent sprint here and opens
    the matching modal directly — no picker needed since that choice was made at join time."""
    sprint = await run_blocking(db_find_loggable_sprint, interaction.channel_id, interaction.user.id)
    if sprint is None:
        await interaction.response.send_message(
            embed=discord.Embed(
                description="You haven't joined a sprint here yet — use `/sprint join` or the Join button first.",
                color=ALERT_COLOR,
            ),
            ephemeral=True,
        )
        return
    modal_cls = LOG_TYPE_MODALS.get(sprint["participant_log_type"])
    if modal_cls is None:
        await interaction.response.send_message(
            embed=discord.Embed(description="Couldn't tell how you're tracking this sprint — try joining again.", color=ALERT_COLOR),
            ephemeral=True,
        )
        return
    await interaction.response.send_modal(modal_cls())


async def refresh_sprint_announcement(sprint_id: int):
    sprint = await run_blocking(db_get_sprint, sprint_id)
    if sprint is None or not sprint.get("announcement_message_id"):
        return
    channel = bot.get_channel(int(sprint["channel_id"]))
    if channel is None:
        return
    try:
        message = await channel.fetch_message(int(sprint["announcement_message_id"]))
    except discord.HTTPException:
        return
    participants = await run_blocking(db_get_participants, sprint_id)
    try:
        await message.edit(embed=build_sprint_announcement_embed(sprint, participants))
    except discord.HTTPException:
        pass


# ===== LOG MODALS (one per tracking type, opened directly — no type picker at log time) =====
class LogPagesModal(discord.ui.Modal, title="Log Pages"):
    pages_input = discord.ui.TextInput(label="Pages read this sprint", placeholder="e.g. 42", max_length=6)

    async def on_submit(self, interaction: discord.Interaction):
        value = _parse_int(self.pages_input.value)
        if value is None or value <= 0:
            await interaction.response.send_message(
                embed=discord.Embed(description="Enter a whole number of pages greater than 0.", color=ALERT_COLOR),
                ephemeral=True,
            )
            return
        await apply_log(interaction, "pages", float(value), float(value), None, f"{EMOJI_PAGE} Logged **{value} pages**.")


class LogPercentageModal(discord.ui.Modal, title="Log Ebook Progress"):
    percent_input = discord.ui.TextInput(label="Percent of book read this sprint", placeholder="e.g. 12.5")
    total_pages_input = discord.ui.TextInput(label="Book's total page count", placeholder="e.g. 320")

    async def on_submit(self, interaction: discord.Interaction):
        percent = _parse_float(self.percent_input.value)
        total_pages = _parse_int(self.total_pages_input.value)
        if percent is None or not (0 < percent <= 100) or total_pages is None or total_pages <= 0:
            await interaction.response.send_message(
                embed=discord.Embed(
                    description="Enter a percent between 0-100 and a total page count greater than 0.",
                    color=ALERT_COLOR,
                ),
                ephemeral=True,
            )
            return
        pages = round(percent / 100 * total_pages, 1)
        await apply_log(
            interaction, "percentage", percent, pages, None,
            f"{EMOJI_EBOOK} Logged **{percent:g}%** of a {total_pages}-page book → **{pages:g} pages**.",
        )


class LogAudioModal(discord.ui.Modal, title="Log Audiobook Progress"):
    percent_input = discord.ui.TextInput(label="Percent listened this sprint", placeholder="e.g. 8")
    hours_input = discord.ui.TextInput(label="Audiobook total length — hours", placeholder="e.g. 8", required=False, default="0")
    minutes_input = discord.ui.TextInput(label="Audiobook total length — minutes", placeholder="e.g. 30", required=False, default="0")

    async def on_submit(self, interaction: discord.Interaction):
        percent = _parse_float(self.percent_input.value)
        hours = _parse_int(self.hours_input.value) or 0
        minutes = _parse_int(self.minutes_input.value) or 0
        total_minutes = hours * 60 + minutes
        if percent is None or not (0 < percent <= 100) or total_minutes <= 0:
            await interaction.response.send_message(
                embed=discord.Embed(
                    description="Enter a percent between 0-100 and the audiobook's total length.",
                    color=ALERT_COLOR,
                ),
                ephemeral=True,
            )
            return
        listened = round(percent / 100 * total_minutes, 1)
        await apply_log(
            interaction, "audio", percent, None, listened,
            f"{EMOJI_AUDIOBOOK} Logged **{percent:g}%** of a {hours}h{minutes}m audiobook → **{listened:g} min**.",
        )


class LogFanficModal(discord.ui.Modal, title="Log Fanfic Words"):
    words_input = discord.ui.TextInput(label="Words read this sprint", placeholder="e.g. 1500")

    async def on_submit(self, interaction: discord.Interaction):
        words = _parse_int(self.words_input.value)
        if words is None or words <= 0:
            await interaction.response.send_message(
                embed=discord.Embed(description="Enter a whole number of words greater than 0.", color=ALERT_COLOR),
                ephemeral=True,
            )
            return
        pages = round(words / FANFIC_WORDS_PER_PAGE, 1)
        await apply_log(
            interaction, "fanfic", float(words), pages, None,
            f"{EMOJI_QUILL} Logged **{words:,} words** → **{pages:g} pages**.",
        )


LOG_TYPE_MODALS = {
    "pages": LogPagesModal,
    "percentage": LogPercentageModal,
    "audio": LogAudioModal,
    "fanfic": LogFanficModal,
}


# ===== JOIN FLOW (pick tracking type, then optional book/fic title) =====
class JoinDetailsModal(discord.ui.Modal, title="Join Sprint"):
    book_title_input = discord.ui.TextInput(label="What are you reading? (optional)", required=False, max_length=100)

    def __init__(self, log_type: str):
        super().__init__()
        self.log_type = log_type

    async def on_submit(self, interaction: discord.Interaction):
        await handle_join(interaction, self.log_type, self.book_title_input.value)


class JoinTypeSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="Pages", value="pages", emoji=EMOJI_PAGE, description="Track by page number"),
            discord.SelectOption(label="Ebook %", value="percentage", emoji=EMOJI_EBOOK, description="Track by percent complete (e.g. Kindle)"),
            discord.SelectOption(label="Audiobook", value="audio", emoji=EMOJI_AUDIOBOOK, description="Track by percent listened"),
            discord.SelectOption(label="Fanfic", value="fanfic", emoji=EMOJI_QUILL, description="Track by word count"),
        ]
        super().__init__(placeholder="How are you tracking this sprint?", options=options)

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(JoinDetailsModal(self.values[0]))


class JoinTypeSelectView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        self.add_item(JoinTypeSelect())


# ===== PERSISTENT VIEWS (attached to real channel messages, re-registered every boot) =====
class SprintJoinView(discord.ui.View):
    """Attached to the sprint announcement — Join picks a tracking type once;
    Update Progress re-opens whichever log modal matches that choice."""

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Join Sprint", emoji=EMOJI_SPRINTING, style=discord.ButtonStyle.green, custom_id="sprintcadia:join_sprint")
    async def join_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "How are you tracking this sprint?", view=JoinTypeSelectView(), ephemeral=True,
        )

    @discord.ui.button(label="Update Progress", emoji=EMOJI_LOG, style=discord.ButtonStyle.grey, custom_id="sprintcadia:update_progress")
    async def update_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await open_log_modal_for_user(interaction)


class SprintLogButtonView(discord.ui.View):
    """Attached to the sprint-end message — the only way to log final progress; no slash command for it."""

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Log Sprint", emoji=EMOJI_LOG, style=discord.ButtonStyle.blurple, custom_id="sprintcadia:log_sprint")
    async def log_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await open_log_modal_for_user(interaction)


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
    participants = await run_blocking(db_get_participants, sprint_id)
    mentions = " ".join(f"<@{p['user_id']}>" for p in participants)
    try:
        await channel.send(
            content=mentions or None,
            embed=build_sprint_end_embed(sprint, participants),
            view=SprintLogButtonView(),
            allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
        )
    except discord.HTTPException as error:
        print("Failed to post sprint end embed:", error)


async def start_sprint(guild_id: int, channel: discord.abc.Messageable, host_id: int, duration_minutes: int) -> bool:
    existing = await run_blocking(db_get_active_sprint, channel.id)
    if existing is not None:
        return False

    sprint_id = await run_blocking(db_create_sprint, guild_id, channel.id, host_id, duration_minutes)
    if sprint_id is None:
        return False

    sprint = await run_blocking(db_get_sprint, sprint_id)
    participants = await run_blocking(db_get_participants, sprint_id)

    role_id = await run_blocking(get_guild_setting, guild_id, SPRINT_ROLE_SETTING_KEY, "")
    content = f"<@&{role_id}>" if role_id else None
    allowed = discord.AllowedMentions(roles=True, users=False, everyone=False) if role_id else discord.AllowedMentions.none()

    message = await channel.send(
        content=content,
        embed=build_sprint_announcement_embed(sprint, participants),
        view=SprintJoinView(),
        allowed_mentions=allowed,
    )
    await run_blocking(db_set_sprint_message, sprint_id, message.id)

    active_sprint_tasks[sprint_id] = bot.loop.create_task(finish_sprint(sprint_id))
    return True


# ===== /sprint COMMANDS =====
sprint_group = app_commands.Group(name="sprint", description="Run a reading sprint in this channel.")


@sprint_group.command(name="start", description="Start a reading sprint in this channel.")
@app_commands.describe(duration_minutes="How long the sprint runs, in minutes")
async def sprint_start(interaction: discord.Interaction, duration_minutes: app_commands.Range[int, 1, 180]):
    started = await start_sprint(interaction.guild_id, interaction.channel, interaction.user.id, duration_minutes)
    if not started:
        await interaction.response.send_message(
            embed=discord.Embed(description="⚠️ There's already an active sprint in this channel.", color=ALERT_COLOR),
            ephemeral=True,
        )
        return
    await interaction.response.send_message("Sprint started!", ephemeral=True)


@sprint_group.command(name="join", description="Join the active sprint in this channel.")
@app_commands.describe(log_type="How you're tracking this sprint", book_title="What you're reading (optional)")
@app_commands.choices(log_type=JOIN_LOG_TYPE_CHOICES)
async def sprint_join(interaction: discord.Interaction, log_type: app_commands.Choice[str], book_title: str = ""):
    await handle_join(interaction, log_type.value, book_title)


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


@sprint_group.command(name="results", description="Show results for the most recent sprint in this channel.")
async def sprint_results(interaction: discord.Interaction):
    sprint = await run_blocking(db_get_active_sprint, interaction.channel_id)
    if sprint is None:
        sprint = await run_blocking(db_find_loggable_sprint, interaction.channel_id, interaction.user.id)
    if sprint is None:
        await interaction.response.send_message(
            embed=discord.Embed(description="No recent sprint found in this channel.", color=ALERT_COLOR),
            ephemeral=True,
        )
        return
    participants = await run_blocking(db_get_participants, sprint["id"])
    await interaction.response.send_message(embed=build_sprint_results_embed(participants))


@sprint_group.command(name="cancel", description="Admin/host only: cancel the active sprint in this channel.")
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


@sprint_group.command(name="set-role", description="Admin only: role to ping when a sprint starts.")
@app_commands.checks.has_permissions(administrator=True)
@app_commands.describe(role="Role to ping whenever a sprint starts in this server")
async def sprint_set_role(interaction: discord.Interaction, role: discord.Role):
    await run_blocking(set_guild_setting, interaction.guild_id, SPRINT_ROLE_SETTING_KEY, str(role.id))
    await interaction.response.send_message(f"✅ Sprints will now ping {role.mention} when they start.", ephemeral=True)


@sprint_group.command(name="clear-role", description="Admin only: stop pinging a role when sprints start.")
@app_commands.checks.has_permissions(administrator=True)
async def sprint_clear_role(interaction: discord.Interaction):
    await run_blocking(set_guild_setting, interaction.guild_id, SPRINT_ROLE_SETTING_KEY, "")
    await interaction.response.send_message("✅ Sprints will no longer ping a role when they start.", ephemeral=True)


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
)
@app_commands.choices(day=WEEKDAY_CHOICES)
async def schedule_add(interaction: discord.Interaction, channel: discord.TextChannel, day: app_commands.Choice[int],
                        time_utc: str, duration_minutes: app_commands.Range[int, 1, 180]):
    if not _valid_time_str(time_utc):
        await interaction.response.send_message(
            embed=discord.Embed(description="⚠️ Time must be in 24h `HH:MM` format, e.g. `18:00`.", color=ALERT_COLOR),
            ephemeral=True,
        )
        return
    await run_blocking(db_add_scheduled_sprint, interaction.guild_id, channel.id, day.value, time_utc,
                       duration_minutes, interaction.user.id)
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
        f"`#{r['id']}` <#{r['channel_id']}> — {WEEKDAY_NAMES[r['day_of_week']]} {r['time_utc']} UTC, {r['duration_minutes']}min"
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
        if channel is None:
            continue
        try:
            await start_sprint(int(row["guild_id"]), channel, bot.user.id, row["duration_minutes"])
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

    # Persistent views need to be re-registered every time the bot restarts.
    bot.add_view(SprintJoinView())
    bot.add_view(SprintLogButtonView())

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
