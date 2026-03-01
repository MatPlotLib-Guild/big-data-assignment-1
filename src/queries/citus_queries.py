import psycopg2
import polars as pl

from src.queries.postgres_queries import _run_sql_file, _run_sql_file_with_result, run_q1, run_q3, run_q4


def connect():
    conn = psycopg2.connect(host="localhost", port=5433, dbname="foursquaredb", user="postgres", password="postgres")
    return conn


def close(conn):
    conn.close()


def run_q2(conn) -> pl.DataFrame:
    result = _run_sql_file_with_result(conn, "src/q2/citus.sql")
    _run_sql_file(conn, "src/q2/citus_cleanup.sql")
    return result


__all__ = ["run_q1", "run_q2", "run_q3", "run_q4", "connect", "close"]
