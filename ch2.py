# ЗАДАЧА 2. Очередь задач для погодного сервиса
#
# Использовать Open-Meteo.
#
# В SQLite создать таблицы:
#
# cities:
# id
# name
# latitude
# longitude
#
# weather:
# id
# city_id
# temperature
# wind_speed
# created_at
#
# Добавить минимум 5 городов.
#
# Нужно:
# 1. Прочитать города из БД.
# 2. Для каждого города создать задачу на получение погоды.
# 3. Положить задачи в queue.Queue.
# 4. Запустить 3 worker-потока.
# 5. Каждый worker:
#    - берёт город из очереди;
#    - делает запрос в Open-Meteo;
#    - получает текущую температуру и скорость ветра;
#    - сохраняет результат в таблицу weather.
# 6. Worker не должен завершаться из-за ошибки одного запроса.
# 7. После выполнения всех задач вывести последнее измерение
#    для каждого города.
#
# Дополнительно:
# если температура изменилась больше чем на 5°C
# относительно предыдущего измерения, вывести:
#
# WARNING: Batumi temperature changed by 6.2°C
#
# Обязательно:
# - queue.Queue
# - threading
# - sqlite3
# - requests
# - обработка ошибок


import sqlite3
import threading
import queue
import requests
import time
from datetime import datetime, timezone

DB_NAME = "weather.db"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
TIMEOUT = 30
NUM_WORKERS = 3
TEMP_CHANGE_THRESHOLD = 5.0
RETRIES = 3
BACKOFF = 2


# ---------- Инициализация БД ----------

def init_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cities (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            name      TEXT NOT NULL UNIQUE,
            latitude  REAL NOT NULL,
            longitude REAL NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            city_id     INTEGER NOT NULL,
            temperature REAL NOT NULL,
            wind_speed  REAL NOT NULL,
            created_at  TEXT NOT NULL,
            FOREIGN KEY (city_id) REFERENCES cities(id)
        )
    """)
    conn.commit()
    return conn


def seed_cities(conn):
    """Добавляем минимум 5 городов, если их ещё нет."""
    cities = [
        ("Moscow",   55.7558, 37.6173),
        ("Batumi",   41.6168, 41.6367),
        ("Berlin",   52.5200, 13.4050),
        ("London",   51.5074, -0.1278),
        ("New York", 40.7128, -74.0060),
    ]
    conn.executemany("""
        INSERT OR IGNORE INTO cities (name, latitude, longitude)
        VALUES (?, ?, ?)
    """, cities)
    conn.commit()


# ---------- Работа с Open-Meteo ----------

def fetch_weather(latitude, longitude, retries=RETRIES, backoff=BACKOFF):
    """Возвращает (temperature, wind_speed) или None при ошибке.
    Использует current_weather=true — актуальный параметр Open-Meteo.
    Повторяет запрос при таймауте/сетевой ошибке."""
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current_weather": "true",
        "timezone": "UTC",
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(OPEN_METEO_URL, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()

            cw = data.get("current_weather")
            if not cw:
                print(f"[BAD DATA] нет current_weather для ({latitude}, {longitude})")
                return None

            temperature = cw.get("temperature")
            wind_speed = cw.get("windspeed")

            if temperature is None or wind_speed is None:
                print(f"[BAD DATA] нет temperature/windspeed для ({latitude}, {longitude})")
                return None

            return float(temperature), float(wind_speed)

        except requests.exceptions.Timeout:
            print(f"[TIMEOUT {attempt}/{retries}] ({latitude}, {longitude})")
        except requests.exceptions.HTTPError as e:
            print(f"[HTTP ERROR] ({latitude}, {longitude}) -> {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"[REQUEST ERROR] ({latitude}, {longitude}) -> {e}")
        except (ValueError, KeyError) as e:
            print(f"[PARSE ERROR] ({latitude}, {longitude}) -> {e}")
            return None

        if attempt < retries:
            time.sleep(backoff * attempt)

    return None


# ---------- Работа с БД ----------

def get_previous_temperature(conn, city_id):
    cursor = conn.execute("""
        SELECT temperature FROM weather
        WHERE city_id = ?
        ORDER BY id DESC
        LIMIT 1
    """, (city_id,))
    row = cursor.fetchone()
    return row[0] if row else None


def save_weather(conn, city_id, temperature, wind_speed):
    conn.execute("""
        INSERT INTO weather (city_id, temperature, wind_speed, created_at)
        VALUES (?, ?, ?, ?)
    """, (
        city_id, temperature, wind_speed,
        datetime.now(timezone.utc).isoformat()
    ))
    conn.commit()


# ---------- Worker ----------

def worker(task_queue, db_lock, stop_event):
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    try:
        while not stop_event.is_set():
            try:
                city = task_queue.get(timeout=1)
            except queue.Empty:
                continue

            if city is None:  # сигнал остановки
                task_queue.task_done()
                break

            city_id, name, latitude, longitude = city
            try:
                result = fetch_weather(latitude, longitude)
                if result is None:
                    print(f"[SKIP] {name}: не удалось получить погоду")
                    continue

                temperature, wind_speed = result

                with db_lock:
                    prev_temp = get_previous_temperature(conn, city_id)
                    save_weather(conn, city_id, temperature, wind_speed)

                print(f"{name} | temp: {temperature}°C | wind: {wind_speed} km/h")

                if prev_temp is not None:
                    diff = abs(temperature - prev_temp)
                    if diff > TEMP_CHANGE_THRESHOLD:
                        print(f"WARNING: {name} temperature changed by {diff:.1f}°C")

            except Exception as e:
                print(f"[WORKER ERROR] {name}: {e}")
            finally:
                task_queue.task_done()
    finally:
        conn.close()


# ---------- Main ----------

def main():
    conn = init_db()
    seed_cities(conn)

    cities = conn.execute(
        "SELECT id, name, latitude, longitude FROM cities"
    ).fetchall()
    conn.close()

    if not cities:
        print("Нет городов в БД")
        return

    print(f"Городов в БД: {len(cities)}. Запускаем {NUM_WORKERS} workers...\n")

    task_queue = queue.Queue()
    for city in cities:
        task_queue.put(city)

    db_lock = threading.Lock()
    stop_event = threading.Event()

    threads = []
    for i in range(NUM_WORKERS):
        t = threading.Thread(
            target=worker,
            args=(task_queue, db_lock, stop_event),
            name=f"worker-{i + 1}",
            daemon=True,
        )
        t.start()
        threads.append(t)

    task_queue.join()

    stop_event.set()
    for _ in threads:
        task_queue.put(None)

    for t in threads:
        t.join(timeout=5)

    print("\n=== Последнее измерение по каждому городу ===")
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.execute("""
        SELECT c.name, w.temperature, w.wind_speed, w.created_at
        FROM cities c
        JOIN weather w ON w.city_id = c.id
        WHERE w.id = (
            SELECT id FROM weather
            WHERE city_id = c.id
            ORDER BY id DESC
            LIMIT 1
        )
        ORDER BY c.name
    """)
    for name, temp, wind, created_at in cursor.fetchall():
        print(f"{name} | temp: {temp}°C | wind: {wind} km/h | {created_at}")
    conn.close()


if __name__ == "__main__":
    main()