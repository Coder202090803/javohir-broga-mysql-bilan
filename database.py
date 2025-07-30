import aiomysql
import os
from dotenv import load_dotenv

load_dotenv()

db_pool = None

async def init_db():
    global db_pool
    db_pool = await aiomysql.create_pool(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        db=os.getenv("DB_NAME"),
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        autocommit=True  # MySQL uchun kerak
    )

    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            # Foydalanuvchilar jadvali
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY
                )
            """)

            # Kodlar jadvali
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS kino_codes (
                    code TEXT PRIMARY KEY,
                    channel TEXT,
                    message_id INTEGER,
                    post_count INTEGER
                )
            """)

            # title ustunini borligini tekshirish va yo‘q bo‘lsa qo‘shish
            await cur.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = 'kino_codes' AND column_name = 'title'
            """)
            result = await cur.fetchone()
            if result[0] == 0:
                await cur.execute("ALTER TABLE kino_codes ADD COLUMN title TEXT")

            # Statistika jadvali
            await cur.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    code TEXT PRIMARY KEY,
                    searched INTEGER DEFAULT 0,
                    viewed INTEGER DEFAULT 0
                )
            """)


# === Foydalanuvchi qo‘shish ===
async def add_user(user_id):
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT IGNORE INTO users (user_id) VALUES (%s)", (user_id,)
            )

# === Foydalanuvchilar soni ===
async def get_user_count():
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT COUNT(*) FROM users")
            (count,) = await cur.fetchone()
            return count

# === Kod qo‘shish ===
async def add_kino_code(code, channel, message_id, post_count, title):
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                INSERT INTO kino_codes (code, channel, message_id, post_count, title)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    channel = VALUES(channel),
                    message_id = VALUES(message_id),
                    post_count = VALUES(post_count),
                    title = VALUES(title)
            """, (code, channel, message_id, post_count, title))

            await cur.execute("""
                INSERT IGNORE INTO stats (code) VALUES (%s)
            """, (code,))

# === Kodni olish ===
async def get_kino_by_code(code):
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT code, channel, message_id, post_count, title
                FROM kino_codes WHERE code = %s
            """, (code,))
            return await cur.fetchone()

# === Barcha kodlarni olish ===
async def get_all_codes():
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT code, title, channel, message_id, post_count FROM kino_codes
            """)
            return await cur.fetchall()

# === Kodni o‘chirish ===
async def delete_kino_code(code):
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("DELETE FROM stats WHERE code = %s", (code,))
            await cur.execute("DELETE FROM kino_codes WHERE code = %s", (code,))
            return cur.rowcount > 0

# === Statistika yangilash ===
async def increment_stat(code, field):
    if field not in ("searched", "viewed", "init"):
        return
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            if field == "init":
                await cur.execute("""
                    INSERT IGNORE INTO stats (code, searched, viewed)
                    VALUES (%s, 0, 0)
                """, (code,))
            else:
                await cur.execute(f"""
                    UPDATE stats SET {field} = {field} + 1 WHERE code = %s
                """, (code,))

# === Statistikani olish ===
async def get_code_stat(code):
    async with db_pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT searched, viewed FROM stats WHERE code = %s
            """, (code,))
            return await cur.fetchone()

# === Kod va title ni yangilash ===
async def update_anime_code(old_code, new_code, new_title):
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE kino_codes SET code = %s, title = %s WHERE code = %s
            """, (new_code, new_title, old_code))

# === Foydalanuvchi IDlarini olish ===
async def get_all_user_ids():
    async with db_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT user_id FROM users")
            rows = await cur.fetchall()
            return [row[0] for row in rows]
