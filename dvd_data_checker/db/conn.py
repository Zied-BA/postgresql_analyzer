import psycopg2
import psycopg2.extras
import logging
from typing import Dict, List, Any, Optional
from contextlib import contextmanager


conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='dvdrental',
                user='postgres',
                password='postgres'
            )
cur = conn.cursor()

cur.execute("SELECT * from dms_schema.category_d")

print(cur.fetchone())