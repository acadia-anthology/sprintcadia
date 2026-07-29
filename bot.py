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
RESULTS_GRACE_SECONDS = 300  # 5 minutes after a sprint ends before results auto-post

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


# Six tracking types: two measurement methods each for ebook and audiobook, plus plain
# pages (physical books) and fanfic (words). "Percent" types need total_reference (book's
# total pages / audiobook's total length) to convert a delta% into pages/minutes; the
# others (pages, ebook_pages, audio_time, fanfic) are direct deltas against start_value.
LOG_TYPE_LABELS = {
    "pages": "Pages",
    "ebook_pages": "Ebook (Pages)",
    "ebook_percent": "Ebook (%)",
    "audio_percent": "Audiobook (%)",
    "audio_time": "Audiobook (Time)",
    "fanfic": "Fanfic",
}
LOG_TYPE_ICONS = {
    "pages": EMOJI_PAGE,
    "ebook_pages": EMOJI_EBOOK,
    "ebook_percent": EMOJI_PERCENTAGE,
    "audio_percent": EMOJI_PERCENTAGE,
    "audio_time": EMOJI_AUDIOBOOK,
    "fanfic": EMOJI_QUILL,
}

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


def _format_minutes_label(total_minutes: float) -> str:
    total = int(round(total_minutes))
    hours, minutes = divmod(total, 60)
    if hours and minutes:
        return f"{hours}h {minutes}m"
    if hours:
        return f"{hours}h"
    return f"{minutes}m"


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
                    announcement_message_id  TEXT DEFAULT '',
                    results_grace_ends_at    TIMESTAMPTZ,
                    results_posted           BOOLEAN NOT NULL DEFAULT FALSE
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
                    start_value        NUMERIC DEFAULT 0,
                    total_reference    NUMERIC,
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


def db_join_sprint(sprint_id: int, user_id: int, log_type: str, book_title: str,
                    start_value: float, total_reference: float | None) -> bool | None:
    """Returns True (joined), False (already in this sprint), or None (a real DB error —
    distinct from False so callers don't misreport an error as 'already joined')."""
    conn = get_pg_connection()
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sprint_participants (sprint_id, user_id, log_type, book_title, start_value, total_reference)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (sprint_id, str(user_id), log_type, book_title, start_value, total_reference))
            joined = cur.rowcount > 0
        conn.commit()
        return joined
    except Exception as error:
        print("db_join_sprint error:", error)
        conn.rollback()
        return None
    finally:
        release_pg_connection(conn)


def db_get_participants(sprint_id: int) -> list[dict]:
    conn = get_pg_connection()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT user_id, log_type, book_title, start_value, total_reference,
                       raw_amount, pages_equivalent, minutes_equivalent, reported
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
    log_type/start_value/total_reference from that sprint so the log button knows which
    modal to open and what to compute the delta against."""
    conn = get_pg_connection()
    if conn is None:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT s.*, p.log_type AS participant_log_type, p.book_title AS participant_book_title,
                       p.start_value AS participant_start_value, p.total_reference AS participant_total_reference
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
                     raw_amount: float, pages_equivalent: float | None, minutes_equivalent: float | None) -> bool:
    """Logging is repeatable (like an 'update progress' action) — each call overwrites the
    participant's prior report and only nudges user_stats by the *difference*, so re-logging
    doesn't double-count pages/minutes already counted from an earlier report this sprint."""
    conn = get_pg_connection()
    if conn is None:
        return False
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
        return True
    except Exception as error:
        print("db_log_progress error:", error)
        conn.rollback()
        return False
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


def db_start_results_grace(sprint_id: int, grace_ends_at: datetime):
    conn = get_pg_connection()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE sprints SET results_grace_ends_at = %s WHERE id = %s", (grace_ends_at, sprint_id))
        conn.commit()
    except Exception as error:
        print("db_start_results_grace error:", error)
        conn.rollback()
    finally:
        release_pg_connection(conn)


def db_mark_results_posted(sprint_id: int):
    conn = get_pg_connection()
    if conn is None:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE sprints SET results_posted = TRUE WHERE id = %s", (sprint_id,))
        conn.commit()
    except Exception as error:
        print("db_mark_results_posted error:", error)
        conn.rollback()
    finally:
        release_pg_connection(conn)


def db_get_sprints_awaiting_results() -> list[dict]:
    conn = get_pg_connection()
    if conn is None:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM sprints WHERE status = 'ended' AND results_posted = FALSE")
            return list(cur.fetchall())
    except Exception as error:
        print("db_get_sprints_awaiting_results error:", error)
        return []
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
def _format_start_value(p: dict) -> str:
    log_type = p.get("log_type")
    start = p.get("start_value")
    total = p.get("total_reference")
    if start is None:
        return ""
    if log_type in ("pages", "ebook_pages"):
        return f"page {int(start)}"
    if log_type == "ebook_percent":
        return f"{float(start):g}%" + (f" (of {int(total)}pg)" if total else "")
    if log_type == "audio_percent":
        return f"{float(start):g}% listened" + (f" (of {_format_minutes_label(total)})" if total else "")
    if log_type == "audio_time":
        return f"{_format_minutes_label(start)} in"
    if log_type == "fanfic":
        return f"{int(start):,} words"
    return ""


def _participant_line(p: dict) -> str:
    icon = LOG_TYPE_ICONS.get(p.get("log_type"), EMOJI_OPENBOOK)
    start_str = _format_start_value(p)
    start_part = f" — {start_str}" if start_str else ""
    title = f" — reading *{p['book_title']}*" if p.get("book_title") else ""
    return f"{icon} <@{p['user_id']}>{start_part}{title}"


def _relative_and_clock(dt: datetime) -> str:
    ts = int(dt.timestamp())
    return f"<t:{ts}:R>\n<t:{ts}:t>"


def build_sprint_announcement_embed(sprint: dict, participants: list[dict]) -> discord.Embed:
    names = "\n".join(_participant_line(p) for p in participants) or "No one yet — be the first!"
    embed = discord.Embed(title=f"{EMOJI_FIRE} Sprint ignited!", color=SPRINT_COLOR)
    embed.add_field(name=f"{EMOJI_TIMER} Duration", value=f"{sprint['duration_minutes']} min", inline=True)
    embed.add_field(name=f"{EMOJI_HOURGLASS} Starts", value=_relative_and_clock(sprint["started_at"]), inline=True)
    embed.add_field(name=f"{EMOJI_HOURGLASS} Ends", value=_relative_and_clock(sprint["ends_at"]), inline=True)
    embed.add_field(name=f"{EMOJI_BOOKSTACK} Participants ({len(participants)})", value=names, inline=False)
    embed.add_field(name=f"{EMOJI_PHOENIXICON} Host", value=f"<@{sprint['host_id']}>", inline=False)
    _set_footer(embed)
    return embed


def build_sprint_status_embed(sprint: dict, participants: list[dict]) -> discord.Embed:
    lines = [_participant_line(p) for p in participants] or ["No one has joined yet."]
    embed = discord.Embed(title=f"{EMOJI_TIMER} Sprint status", color=SPRINT_COLOR)
    embed.add_field(name=f"{EMOJI_HOURGLASS} Ends", value=_relative_and_clock(sprint["ends_at"]), inline=True)
    embed.add_field(name=f"{EMOJI_BOOKSTACK} Participants ({len(participants)})", value="\n".join(lines), inline=False)
    _set_footer(embed)
    return embed


def build_sprint_end_embed(sprint: dict, participants: list[dict]) -> discord.Embed:
    names = "\n".join(_participant_line(p) for p in participants) or "No one joined this one."
    grace_minutes = RESULTS_GRACE_SECONDS // 60
    embed = discord.Embed(
        title=f"{EMOJI_PHOENIX} Sprint complete — rise & report!",
        description=(
            f"The {sprint['duration_minutes']}-minute sprint is over. Tap **Log Sprint** below.\n"
            f"Results post automatically in {grace_minutes} minutes — sooner if everyone's logged in."
        ),
        color=RESULT_COLOR,
    )
    embed.add_field(name=f"{EMOJI_BOOKSTACK} Participants", value=names, inline=False)
    _set_footer(embed)
    return embed


MEDALS = ["🥇", "🥈", "🥉"]


def _format_result_line(p: dict, rank_prefix: str = "") -> str:
    uid = p["user_id"]
    title = f" *({p['book_title']})*" if p.get("book_title") else ""
    if not p["reported"]:
        start_str = _format_start_value(p)
        started = f" (started at {start_str})" if start_str else ""
        return f"{rank_prefix}<@{uid}>{title}{started} — no report yet"

    log_type = p["log_type"]
    raw = p["raw_amount"]
    if log_type in ("pages", "ebook_pages"):
        return f"{rank_prefix}<@{uid}>{title} — page **{int(raw)}** (**{float(p['pages_equivalent']):+g} pages**)"
    if log_type == "ebook_percent":
        return f"{rank_prefix}<@{uid}>{title} — **{float(raw):g}%** (**{float(p['pages_equivalent']):+g} pages**)"
    if log_type == "audio_percent":
        return f"{rank_prefix}<@{uid}>{title} — **{float(raw):g}%** listened (**{float(p['minutes_equivalent']):+g} min**)"
    if log_type == "audio_time":
        return f"{rank_prefix}<@{uid}>{title} — **{_format_minutes_label(raw)} in** (**{float(p['minutes_equivalent']):+g} min**)"
    if log_type == "fanfic":
        return f"{rank_prefix}<@{uid}>{title} — **{int(raw):,} words** (**{float(p['pages_equivalent']):+g} pages**)"
    return f"{rank_prefix}<@{uid}>{title} — reported"


def build_sprint_results_embed(participants: list[dict]) -> discord.Embed:
    # db_get_participants already orders reported-first, highest pages_equivalent first —
    # ranking here is purely cosmetic (medal prefixes), not a re-sort.
    reported = [p for p in participants if p["reported"]]
    unreported = [p for p in participants if not p["reported"]]

    total_pages = sum(float(p["pages_equivalent"]) for p in reported if p["pages_equivalent"] is not None)
    total_minutes = sum(float(p["minutes_equivalent"]) for p in reported if p["minutes_equivalent"] is not None)

    ranked_lines = [
        _format_result_line(p, MEDALS[i] + " " if i < len(MEDALS) else f"{i + 1}. ")
        for i, p in enumerate(reported)
    ]

    embed = discord.Embed(title=f"{EMOJI_PHOENIXICON} Sprint results", color=RESULT_COLOR)
    embed.add_field(
        name="Reported",
        value="\n".join(ranked_lines) or "No one has reported yet.",
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
    lines = []
    for i, row in enumerate(rows):
        prefix = MEDALS[i] if i < len(MEDALS) else f"{i + 1}."
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
grace_tasks: dict[int, asyncio.Task] = {}


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
    ok = await run_blocking(db_log_progress, sprint["id"], interaction.user.id, interaction.guild_id,
                             log_type, raw_amount, pages_equivalent, minutes_equivalent)
    if not ok:
        await interaction.response.send_message(
            embed=discord.Embed(description="⚠️ Something went wrong saving that — please try again.", color=ALERT_COLOR),
            ephemeral=True,
        )
        return
    await interaction.response.send_message(f"{interaction.user.mention} {confirm_text}")

    # If the sprint's already in its post-end grace window and everyone's now reported,
    # post the results board right away instead of waiting out the rest of the window.
    if sprint["status"] == "ended" and not sprint.get("results_posted"):
        participants = await run_blocking(db_get_participants, sprint["id"])
        if participants and all(p["reported"] for p in participants):
            task = grace_tasks.pop(sprint["id"], None)
            if task is not None:
                task.cancel()
            bot.loop.create_task(post_sprint_results(sprint["id"]))


async def handle_join(interaction: discord.Interaction, log_type: str, book_title: str,
                       start_value: float, total_reference: float | None):
    sprint = await run_blocking(db_get_active_sprint, interaction.channel_id)
    if sprint is None:
        await interaction.response.send_message(
            embed=discord.Embed(description="No active sprint here. Start one with `/sprint start`.", color=ALERT_COLOR),
            ephemeral=True,
        )
        return
    joined = await run_blocking(db_join_sprint, sprint["id"], interaction.user.id, log_type,
                                 book_title.strip(), start_value, total_reference)
    if joined is None:
        await interaction.response.send_message(
            embed=discord.Embed(description="⚠️ Something went wrong saving that — please try again.", color=ALERT_COLOR),
            ephemeral=True,
        )
        return
    if joined:
        start_str = _format_start_value({"log_type": log_type, "start_value": start_value, "total_reference": total_reference})
        extra = f" — reading *{book_title.strip()}*" if book_title.strip() else ""
        await interaction.response.send_message(f"{EMOJI_PIN} {interaction.user.mention} joined at **{start_str}**{extra}!")
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
    log_type = sprint["participant_log_type"]
    factory = LOG_MODAL_FACTORIES.get(log_type)
    if factory is None:
        await interaction.response.send_message(
            embed=discord.Embed(description="Couldn't tell how you're tracking this sprint — try joining again.", color=ALERT_COLOR),
            ephemeral=True,
        )
        return
    start_value = float(sprint["participant_start_value"]) if sprint.get("participant_start_value") is not None else 0.0
    total_reference = float(sprint["participant_total_reference"]) if sprint.get("participant_total_reference") is not None else None
    await interaction.response.send_modal(factory(start_value, total_reference))


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


async def post_sprint_results(sprint_id: int):
    sprint = await run_blocking(db_get_sprint, sprint_id)
    if sprint is None or sprint.get("results_posted"):
        return
    await run_blocking(db_mark_results_posted, sprint_id)
    grace_tasks.pop(sprint_id, None)
    channel = bot.get_channel(int(sprint["channel_id"]))
    if channel is None:
        return
    participants = await run_blocking(db_get_participants, sprint_id)
    try:
        await channel.send(embed=build_sprint_results_embed(participants))
    except discord.HTTPException as error:
        print("Failed to post sprint results:", error)


async def run_results_grace_period(sprint_id: int, seconds: float):
    if seconds > 0:
        await asyncio.sleep(seconds)
    await post_sprint_results(sprint_id)


# ===== JOIN MODALS (one per tracking type — captures a starting point + optional title) =====
class JoinPageBasedModal(discord.ui.Modal):
    book_title_input = discord.ui.TextInput(label="What are you reading? (optional)", required=False, max_length=100)
    starting_page_input = discord.ui.TextInput(label="What page are you starting on?", placeholder="e.g. 74", default="0")

    def __init__(self, log_type: str):
        super().__init__(title=f"Join Sprint — {LOG_TYPE_LABELS[log_type]}")
        self.log_type = log_type

    async def on_submit(self, interaction: discord.Interaction):
        start = _parse_int(self.starting_page_input.value)
        if start is None or start < 0:
            await interaction.response.send_message(
                embed=discord.Embed(description="Enter a starting page number (0 or higher).", color=ALERT_COLOR),
                ephemeral=True,
            )
            return
        await handle_join(interaction, self.log_type, self.book_title_input.value, float(start), None)


class JoinEbookPercentModal(discord.ui.Modal, title="Join Sprint — Ebook (%)"):
    book_title_input = discord.ui.TextInput(label="What are you reading? (optional)", required=False, max_length=100)
    starting_percent_input = discord.ui.TextInput(label="What percent are you starting at?", placeholder="e.g. 30", default="0")
    total_pages_input = discord.ui.TextInput(label="Book's total page count", placeholder="e.g. 320")

    async def on_submit(self, interaction: discord.Interaction):
        start = _parse_float(self.starting_percent_input.value)
        total_pages = _parse_int(self.total_pages_input.value)
        if start is None or not (0 <= start <= 100) or total_pages is None or total_pages <= 0:
            await interaction.response.send_message(
                embed=discord.Embed(
                    description="Enter a starting percent (0-100) and a total page count greater than 0.",
                    color=ALERT_COLOR,
                ),
                ephemeral=True,
            )
            return
        await handle_join(interaction, "ebook_percent", self.book_title_input.value, start, float(total_pages))


class JoinAudioPercentModal(discord.ui.Modal, title="Join Sprint — Audiobook (%)"):
    book_title_input = discord.ui.TextInput(label="What are you listening to? (optional)", required=False, max_length=100)
    starting_percent_input = discord.ui.TextInput(label="Percent listened so far", placeholder="e.g. 10", default="0")
    total_hours_input = discord.ui.TextInput(label="Audiobook total length — hours", required=False, default="0")
    total_minutes_input = discord.ui.TextInput(label="Audiobook total length — minutes", required=False, default="0")

    async def on_submit(self, interaction: discord.Interaction):
        start = _parse_float(self.starting_percent_input.value)
        hours = _parse_int(self.total_hours_input.value) or 0
        minutes = _parse_int(self.total_minutes_input.value) or 0
        total = hours * 60 + minutes
        if start is None or not (0 <= start <= 100) or total <= 0:
            await interaction.response.send_message(
                embed=discord.Embed(
                    description="Enter a starting percent (0-100) and the audiobook's total length.",
                    color=ALERT_COLOR,
                ),
                ephemeral=True,
            )
            return
        await handle_join(interaction, "audio_percent", self.book_title_input.value, start, float(total))


class JoinAudioTimeModal(discord.ui.Modal, title="Join Sprint — Audiobook (Time)"):
    book_title_input = discord.ui.TextInput(label="What are you listening to? (optional)", required=False, max_length=100)
    starting_hours_input = discord.ui.TextInput(label="Starting point — hours in", required=False, default="0")
    starting_minutes_input = discord.ui.TextInput(label="Starting point — minutes in", required=False, default="0")

    async def on_submit(self, interaction: discord.Interaction):
        hours = _parse_int(self.starting_hours_input.value) or 0
        minutes = _parse_int(self.starting_minutes_input.value) or 0
        if hours < 0 or minutes < 0:
            await interaction.response.send_message(
                embed=discord.Embed(description="Enter a starting time of 0 or higher.", color=ALERT_COLOR),
                ephemeral=True,
            )
            return
        await handle_join(interaction, "audio_time", self.book_title_input.value, float(hours * 60 + minutes), None)


class JoinFanficModal(discord.ui.Modal, title="Join Sprint — Fanfic"):
    fic_title_input = discord.ui.TextInput(label="Fic title (optional)", required=False, max_length=100)
    starting_words_input = discord.ui.TextInput(label="What's your starting word count?", placeholder="e.g. 0", default="0")

    async def on_submit(self, interaction: discord.Interaction):
        start = _parse_int(self.starting_words_input.value)
        if start is None or start < 0:
            await interaction.response.send_message(
                embed=discord.Embed(description="Enter a starting word count of 0 or higher.", color=ALERT_COLOR),
                ephemeral=True,
            )
            return
        await handle_join(interaction, "fanfic", self.fic_title_input.value, float(start), None)


JOIN_MODAL_FACTORIES = {
    "pages": lambda: JoinPageBasedModal("pages"),
    "ebook_pages": lambda: JoinPageBasedModal("ebook_pages"),
    "ebook_percent": lambda: JoinEbookPercentModal(),
    "audio_percent": lambda: JoinAudioPercentModal(),
    "audio_time": lambda: JoinAudioTimeModal(),
    "fanfic": lambda: JoinFanficModal(),
}


class JoinTypeSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="Pages", value="pages", emoji=EMOJI_PAGE,
                                  description="Physical/print book — track by page number"),
            discord.SelectOption(label="Ebook (Pages)", value="ebook_pages", emoji=EMOJI_EBOOK,
                                  description="E-reader that shows a page number"),
            discord.SelectOption(label="Ebook (%)", value="ebook_percent", emoji=EMOJI_PERCENTAGE,
                                  description="E-reader that shows percent complete (e.g. Kindle)"),
            discord.SelectOption(label="Audiobook (%)", value="audio_percent", emoji=EMOJI_PERCENTAGE,
                                  description="Track by percent listened"),
            discord.SelectOption(label="Audiobook (Time)", value="audio_time", emoji=EMOJI_AUDIOBOOK,
                                  description="Track by elapsed listening time"),
            discord.SelectOption(label="Fanfic", value="fanfic", emoji=EMOJI_QUILL,
                                  description="Track by word count (250 words ≈ 1 page)"),
        ]
        super().__init__(placeholder="How are you tracking this sprint?", options=options)

    async def callback(self, interaction: discord.Interaction):
        modal = JOIN_MODAL_FACTORIES[self.values[0]]()
        await interaction.response.send_modal(modal)


class JoinTypeSelectView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        self.add_item(JoinTypeSelect())


# ===== LOG MODALS (current value only — deltas computed against the stored start_value) =====
class LogPageBasedModal(discord.ui.Modal):
    current_page_input = discord.ui.TextInput(label="What page are you on now?", placeholder="e.g. 146")

    def __init__(self, log_type: str, start_value: float):
        super().__init__(title=f"Log {LOG_TYPE_LABELS[log_type]}")
        self.log_type = log_type
        self.start_value = start_value

    async def on_submit(self, interaction: discord.Interaction):
        current = _parse_int(self.current_page_input.value)
        if current is None or current < 0:
            await interaction.response.send_message(
                embed=discord.Embed(description="Enter a whole page number.", color=ALERT_COLOR), ephemeral=True,
            )
            return
        delta = current - self.start_value
        icon = LOG_TYPE_ICONS[self.log_type]
        await apply_log(interaction, self.log_type, float(current), float(delta), None,
                         f"{icon} logged **page {current}** (**{delta:+g} pages** this sprint).")


class LogEbookPercentModal(discord.ui.Modal, title="Log Ebook Progress"):
    current_percent_input = discord.ui.TextInput(label="What percent are you at now?", placeholder="e.g. 62")

    def __init__(self, start_value: float, total_reference: float | None):
        super().__init__()
        self.start_value = start_value
        self.total_reference = total_reference or 0

    async def on_submit(self, interaction: discord.Interaction):
        current = _parse_float(self.current_percent_input.value)
        if current is None or not (0 <= current <= 100):
            await interaction.response.send_message(
                embed=discord.Embed(description="Enter a percent between 0-100.", color=ALERT_COLOR), ephemeral=True,
            )
            return
        delta_percent = current - self.start_value
        pages = round(delta_percent / 100 * self.total_reference, 1)
        await apply_log(interaction, "ebook_percent", current, pages, None,
                         f"{EMOJI_PERCENTAGE} logged **{current:g}%** (**{pages:+g} pages** this sprint).")


class LogAudioPercentModal(discord.ui.Modal, title="Log Audiobook Progress (%)"):
    current_percent_input = discord.ui.TextInput(label="What percent have you listened to now?", placeholder="e.g. 45")

    def __init__(self, start_value: float, total_reference: float | None):
        super().__init__()
        self.start_value = start_value
        self.total_reference = total_reference or 0

    async def on_submit(self, interaction: discord.Interaction):
        current = _parse_float(self.current_percent_input.value)
        if current is None or not (0 <= current <= 100):
            await interaction.response.send_message(
                embed=discord.Embed(description="Enter a percent between 0-100.", color=ALERT_COLOR), ephemeral=True,
            )
            return
        delta_percent = current - self.start_value
        minutes = round(delta_percent / 100 * self.total_reference, 1)
        await apply_log(interaction, "audio_percent", current, None, minutes,
                         f"{EMOJI_PERCENTAGE} logged **{current:g}% listened** (**{minutes:+g} min** this sprint).")


class LogAudioTimeModal(discord.ui.Modal, title="Log Audiobook Progress (Time)"):
    current_hours_input = discord.ui.TextInput(label="Current point — hours in", required=False, default="0")
    current_minutes_input = discord.ui.TextInput(label="Current point — minutes in", required=False, default="0")

    def __init__(self, start_value: float, total_reference: float | None):
        super().__init__()
        self.start_value = start_value

    async def on_submit(self, interaction: discord.Interaction):
        hours = _parse_int(self.current_hours_input.value) or 0
        minutes = _parse_int(self.current_minutes_input.value) or 0
        if hours < 0 or minutes < 0:
            await interaction.response.send_message(
                embed=discord.Embed(description="Enter a valid elapsed time.", color=ALERT_COLOR), ephemeral=True,
            )
            return
        current_total = hours * 60 + minutes
        delta = current_total - self.start_value
        await apply_log(interaction, "audio_time", float(current_total), None, float(delta),
                         f"{EMOJI_AUDIOBOOK} logged **{_format_minutes_label(current_total)} in** (**{delta:+g} min** this sprint).")


class LogFanficModal(discord.ui.Modal, title="Log Fanfic Words"):
    current_words_input = discord.ui.TextInput(label="What's your word count now?", placeholder="e.g. 3200")

    def __init__(self, start_value: float, total_reference: float | None):
        super().__init__()
        self.start_value = start_value

    async def on_submit(self, interaction: discord.Interaction):
        current = _parse_int(self.current_words_input.value)
        if current is None or current < 0:
            await interaction.response.send_message(
                embed=discord.Embed(description="Enter a whole word count.", color=ALERT_COLOR), ephemeral=True,
            )
            return
        delta = current - self.start_value
        pages = round(delta / FANFIC_WORDS_PER_PAGE, 1)
        await apply_log(interaction, "fanfic", float(current), pages, None,
                         f"{EMOJI_QUILL} logged **{current:,} words** (**{pages:+g} pages** this sprint).")


LOG_MODAL_FACTORIES = {
    "pages": lambda start, total: LogPageBasedModal("pages", start),
    "ebook_pages": lambda start, total: LogPageBasedModal("ebook_pages", start),
    "ebook_percent": lambda start, total: LogEbookPercentModal(start, total),
    "audio_percent": lambda start, total: LogAudioPercentModal(start, total),
    "audio_time": lambda start, total: LogAudioTimeModal(start, total),
    "fanfic": lambda start, total: LogFanficModal(start, total),
}


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
    """Waits out the remaining sprint time, posts the 'log your progress' message, then
    arms a grace-period timer that auto-posts results in RESULTS_GRACE_SECONDS (or sooner
    if everyone's reported already — see apply_log)."""
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
    if channel is not None:
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

    grace_deadline = now_utc() + timedelta(seconds=RESULTS_GRACE_SECONDS)
    await run_blocking(db_start_results_grace, sprint_id, grace_deadline)
    grace_tasks[sprint_id] = bot.loop.create_task(run_results_grace_period(sprint_id, RESULTS_GRACE_SECONDS))


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


# ===== /sprint join SUBCOMMANDS (one per tracking type, matching the button/modal fields) =====
join_group = app_commands.Group(name="join", description="Join the active sprint in this channel.", parent=sprint_group)


@join_group.command(name="pages", description="Join tracking a physical book by page number.")
@app_commands.describe(starting_page="What page are you starting on?", book_title="What you're reading (optional)")
async def join_pages(interaction: discord.Interaction, starting_page: app_commands.Range[int, 0, None], book_title: str = ""):
    await handle_join(interaction, "pages", book_title, float(starting_page), None)


@join_group.command(name="ebook-pages", description="Join tracking an ebook by page number.")
@app_commands.describe(starting_page="What page are you starting on?", book_title="What you're reading (optional)")
async def join_ebook_pages(interaction: discord.Interaction, starting_page: app_commands.Range[int, 0, None], book_title: str = ""):
    await handle_join(interaction, "ebook_pages", book_title, float(starting_page), None)


@join_group.command(name="ebook-percent", description="Join tracking an ebook by percent complete.")
@app_commands.describe(
    starting_percent="What percent are you starting at?",
    total_pages="Book's total page count",
    book_title="What you're reading (optional)",
)
async def join_ebook_percent(interaction: discord.Interaction, starting_percent: app_commands.Range[float, 0, 100],
                              total_pages: app_commands.Range[int, 1, None], book_title: str = ""):
    await handle_join(interaction, "ebook_percent", book_title, starting_percent, float(total_pages))


@join_group.command(name="audio-percent", description="Join tracking an audiobook by percent listened.")
@app_commands.describe(
    starting_percent="Percent listened so far",
    total_hours="Audiobook total length — hours",
    total_minutes="Audiobook total length — minutes",
    book_title="What you're listening to (optional)",
)
async def join_audio_percent(interaction: discord.Interaction, starting_percent: app_commands.Range[float, 0, 100],
                              total_hours: app_commands.Range[int, 0, None] = 0, total_minutes: app_commands.Range[int, 0, 59] = 0,
                              book_title: str = ""):
    total = total_hours * 60 + total_minutes
    if total <= 0:
        await interaction.response.send_message(
            embed=discord.Embed(description="Enter the audiobook's total length.", color=ALERT_COLOR), ephemeral=True,
        )
        return
    await handle_join(interaction, "audio_percent", book_title, starting_percent, float(total))


@join_group.command(name="audio-time", description="Join tracking an audiobook by elapsed listening time.")
@app_commands.describe(
    starting_hours="Starting point — hours in",
    starting_minutes="Starting point — minutes in",
    book_title="What you're listening to (optional)",
)
async def join_audio_time(interaction: discord.Interaction, starting_hours: app_commands.Range[int, 0, None] = 0,
                           starting_minutes: app_commands.Range[int, 0, 59] = 0, book_title: str = ""):
    await handle_join(interaction, "audio_time", book_title, float(starting_hours * 60 + starting_minutes), None)


@join_group.command(name="fanfic", description="Join tracking a fanfic by word count.")
@app_commands.describe(starting_words="Your starting word count", fic_title="Fic title (optional)")
async def join_fanfic(interaction: discord.Interaction, starting_words: app_commands.Range[int, 0, None] = 0, fic_title: str = ""):
    await handle_join(interaction, "fanfic", fic_title, float(starting_words), None)


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

    # Re-arm results grace-period timers for sprints that ended while the bot was offline.
    awaiting_results = await run_blocking(db_get_sprints_awaiting_results)
    for sprint in awaiting_results:
        grace_ends_at = sprint.get("results_grace_ends_at")
        remaining = max((grace_ends_at - now_utc()).total_seconds(), 0) if grace_ends_at else 0
        grace_tasks[sprint["id"]] = bot.loop.create_task(run_results_grace_period(sprint["id"], remaining))
    if awaiting_results:
        print(f"Re-armed {len(awaiting_results)} results grace period(s).")

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
