import sqlite3

conn = sqlite3.connect(
    "apps/res-premises/factory_webserver/services/warehouse/db/warehouse.db"
)

c = conn.cursor()

conn.execute(
    """CREATE TABLE rolls_today (
             roll_id text UNIQUE,
             location text,
             roll_name text,
             status text NULL,
             date date)"""
)
