#!/usr/bin/env python3
"""
config.py

- Tạo schema `controller` nếu chưa có
- Tạo bảng `controller.app_config` (versioning) nếu chưa có
- Tạo bảng `controller.log` để ghi log
- Đọc file JSON config (mặc định: ./lottery_config.json)
- Tính version mới và INSERT (không update bản cũ)
- Luôn cố gắng ghi log SUCCESS / FAIL vào controller.log

Usage (ví dụ chạy trên host):

  export PGHOST=localhost
  export PGPORT=5432
  export PGDATABASE=n8n_data
  export PGUSER=n8n
  export PGPASSWORD=n8n_pass

  python3 config.py
"""

import json
import psycopg2
import os
import sys

# --- Configurable ---
CONFIG_PATH = os.getenv("CONFIG_PATH", "./config_lottery.json")
CONFIG_NAME = os.getenv("CONFIG_NAME", "lottery")  # logical name trong app_config.name
# --------------------


def get_raw_conn():
    """
    Kết nối DB, KHÔNG bắt lỗi ở đây, để nơi gọi chủ động catch.
    """
    host = os.getenv("PGHOST", "postgres")
    port = int(os.getenv("PGPORT", 5432))
    db = os.getenv("PGDATABASE", "n8n_data")
    user = os.getenv("PGUSER", "n8n")
    password = os.getenv("PGPASSWORD", "n8n_pass")
    conn = psycopg2.connect(
        host=host, port=port, dbname=db, user=user, password=password
    )
    return conn


def ensure_log_table(conn):
    """
    Đảm bảo schema controller + bảng controller.log tồn tại.
    Dùng connection đã có.
    """
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS controller;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS controller.log (
              id SERIAL PRIMARY KEY,
              action TEXT NOT NULL,
              status TEXT NOT NULL,          -- 'SUCCESS' / 'FAIL'
              message TEXT,
              created_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )
    conn.commit()


def log_best_effort(action: str, status: str, message: str):
    """
    Cố gắng ghi log vào controller.log.
    Nếu lỗi (DB chết, schema chưa tạo, v.v.) thì chỉ print cảnh báo, không raise thêm.
    """
    try:
        conn = get_raw_conn()
    except Exception as e:
        print("⚠️ Không thể connect DB để ghi log:", e)
        print(f"⚠️ BỎ QUA log [{action} - {status}] message={message}")
        return

    try:
        ensure_log_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO controller.log (action, status, message)
                VALUES (%s, %s, %s);
                """,
                (action, status, message),
            )
        conn.commit()
    except Exception as e:
        print("⚠️ Lỗi khi ghi log vào controller.log:", e)
        print(f"⚠️ BỎ QUA log [{action} - {status}] message={message}")
    finally:
        conn.close()


def read_config(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except FileNotFoundError as e:
        msg = f"Không tìm thấy file config: {path}"
        print("❌", msg)
        log_best_effort("INIT_CONFIG_READ_FILE", "FAIL", msg)
        sys.exit(1)
    except json.JSONDecodeError as e:
        msg = f"Lỗi JSON trong file config {path}: {e}"
        print("❌", msg)
        log_best_effort("INIT_CONFIG_READ_FILE", "FAIL", msg)
        sys.exit(1)


def ensure_schema_and_table(cur):
    """
    Đảm bảo:
    - schema controller
    - bảng controller.app_config
    - bảng controller.log
    đã tồn tại.
    """
    # Schema controller
    cur.execute("CREATE SCHEMA IF NOT EXISTS controller;")

    # Bảng log (nếu chưa tạo)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS controller.log (
          id SERIAL PRIMARY KEY,
          action TEXT NOT NULL,
          status TEXT NOT NULL,
          message TEXT,
          created_at TIMESTAMPTZ DEFAULT now()
        );
        """
    )

    # Bảng versioned config
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS controller.app_config (
          id SERIAL PRIMARY KEY,
          name TEXT NOT NULL,
          version INT NOT NULL,
          config JSONB NOT NULL,
          created_at TIMESTAMPTZ DEFAULT now(),
          UNIQUE (name, version)
        );
        """
    )

    # Index để query bản mới nhất nhanh hơn
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_app_config_name_version
        ON controller.app_config (name, version DESC);
        """
    )


def get_next_version(cur, name: str):
    cur.execute(
        "SELECT COALESCE(MAX(version), 0) FROM controller.app_config WHERE name = %s",
        (name,),
    )
    row = cur.fetchone()
    latest = row[0] if row and row[0] is not None else 0
    return latest + 1, latest


def insert_new_version(cur, name: str, version: int, cfg_json: dict):
    cur.execute(
        "INSERT INTO controller.app_config (name, version, config) VALUES (%s, %s, %s::jsonb)",
        (name, version, json.dumps(cfg_json)),
    )


def main():
    print("====================================")
    print("📌 INIT CONFIG (SCHEMA: controller)")
    print("====================================")

    # 1) Đọc file config
    cfg = read_config(CONFIG_PATH)
    print(f"📄 Đã đọc file config: {CONFIG_PATH}")

    # 2) Kết nối DB
    try:
        conn = get_raw_conn()
        print("🔌 Kết nối tới PostgreSQL thành công.")
    except Exception as e:
        msg = f"Không thể kết nối tới database: {e}"
        print("❌", msg)
        log_best_effort("INIT_CONFIG_DB_CONNECT", "FAIL", msg)
        sys.exit(1)

    try:
        cur = conn.cursor()

        # 3) Đảm bảo schema + bảng
        ensure_schema_and_table(cur)
        conn.commit()
        print(
            "📦 Schema 'controller' + bảng 'controller.app_config' & 'controller.log' đã sẵn sàng."
        )

        # 4) Tính version mới
        next_version, latest_version = get_next_version(cur, CONFIG_NAME)
        print(f"🔍 Version hiện tại cho '{CONFIG_NAME}': {latest_version}")
        print(f"✨ Sẽ tạo bản mới version: {next_version}")

        # 5) Insert app_config
        insert_new_version(cur, CONFIG_NAME, next_version, cfg)
        conn.commit()
        print(
            f"🎉 Đã chèn config mới vào controller.app_config (name='{CONFIG_NAME}', version={next_version})"
        )

        # 6) Ghi log SUCCESS
        log_best_effort(
            "INIT_CONFIG",
            "SUCCESS",
            f"Inserted config version {next_version} cho '{CONFIG_NAME}'",
        )

    except Exception as e:
        msg = f"Lỗi khi thao tác với database: {e}"
        print("❌", msg)
        conn.rollback()
        # Ghi log FAIL
        log_best_effort("INIT_CONFIG", "FAIL", msg)
        sys.exit(1)
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()

    print("====================================")
    print(
        "🏁 HOÀN TẤT: Config versioning + logging đã sẵn sàng trong schema 'controller'"
    )
    print("====================================")


if __name__ == "__main__":
    main()
