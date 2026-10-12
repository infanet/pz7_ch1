# ЗАДАЧА 1. Многопоточный сборщик статистики
#
# Использовать JSONPlaceholder:
# /users
# /posts?userId={id}
# /comments?postId={id}
#
# Нужно:
# 1. Получить всех пользователей.
# 2. Для каждого пользователя отдельный worker должен:
#    - получить его посты;
#    - получить комментарии к каждому посту;
#    - посчитать количество постов и комментариев.
# 3. Одновременно должно работать максимум 5 workers.
# 4. Результат сохранить в SQLite:
#
# user_stats:
# id
# user_id
# name
# posts_count
# comments_count
# updated_at
#
# 5. Повторный запуск не должен создавать дубликаты:
#    существующая статистика должна обновляться.
# 6. В конце вывести TOP-3 пользователей по количеству комментариев.
#
# Обязательно:
# - requests
# - ThreadPoolExecutor
# - sqlite3
# - timeout и обработка ошибок HTTP
#
# Пример:
# Leanne Graham | posts: 10 | comments: 50

import sqlite3
import requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://jsonplaceholder.typicode.com"
TIMEOUT = 30
MAX_WORKERS = 5
DB_NAME = "stats.db"


def get_json(url, retries=3, backoff=2):
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            print(f"[TIMEOUT {attempt}/{retries}] {url}")
        except requests.exceptions.HTTPError as e:
            print(f"[HTTP ERROR] {url} -> {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"[REQUEST ERROR] {url} -> {e}")
        if attempt < retries:
            import time
            time.sleep(backoff * attempt)
    return None


def init_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_stats (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id        INTEGER UNIQUE,
            name           TEXT NOT NULL,
            posts_count    INTEGER NOT NULL DEFAULT 0,
            comments_count INTEGER NOT NULL DEFAULT 0,
            updated_at     TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def save_stats(conn, user_id, name, posts_count, comments_count):
    conn.execute("""
        INSERT INTO user_stats (user_id, name, posts_count, comments_count, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            name = excluded.name,
            posts_count = excluded.posts_count,
            comments_count = excluded.comments_count,
            updated_at = excluded.updated_at
    """, (
        user_id, name, posts_count, comments_count,
        datetime.now(timezone.utc).isoformat()
    ))
    conn.commit()


def process_user(user, posts_by_user, comments_by_post):
    user_id = user["id"]
    name = user["name"]

    posts = posts_by_user.get(user_id, [])
    posts_count = len(posts)

    comments_count = 0
    for post in posts:
        comments_count += len(comments_by_post.get(post["id"], []))

    return user_id, name, posts_count, comments_count


def main():
    conn = init_db()

    print("Загружаем пользователей...")
    users = get_json(f"{BASE_URL}/users")
    if not users:
        print("Не удалось получить пользователей")
        return

    print("Загружаем посты...")
    all_posts = get_json(f"{BASE_URL}/posts") or []

    print("Загружаем комментарии...")
    all_comments = get_json(f"{BASE_URL}/comments") or []

    posts_by_user = {}
    for p in all_posts:
        posts_by_user.setdefault(p["userId"], []).append(p)

    comments_by_post = {}
    for c in all_comments:
        comments_by_post.setdefault(c["postId"], []).append(c)

    print(f"Пользователей: {len(users)}. Запускаем {MAX_WORKERS} workers...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_user, u, posts_by_user, comments_by_post): u
            for u in users
        }

        for future in as_completed(futures):
            result = future.result()
            if result is None:
                continue
            user_id, name, posts_count, comments_count = result
            save_stats(conn, user_id, name, posts_count, comments_count)
            print(f"{name} | posts: {posts_count} | comments: {comments_count}")

    print("\n=== TOP-3 по комментариям ===")
    cursor = conn.execute("""
        SELECT name, posts_count, comments_count
        FROM user_stats
        ORDER BY comments_count DESC, name ASC
        LIMIT 3
    """)
    for row in cursor.fetchall():
        print(f"{row[0]} | posts: {row[1]} | comments: {row[2]}")

    conn.close()


if __name__ == "__main__":
    main()