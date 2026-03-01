import psycopg2
import polars as pl


def connect():
    return psycopg2.connect(host="localhost", port=5432, dbname="foursquaredb", user="postgres", password="postgres")


def close(conn):
    conn.close()


def _run_sql_file(conn, file_path):
    with open(file_path, "r") as f:
        query = f.read()
    cur = conn.cursor()
    cur.execute(query)
    cur.close()


def _run_sql_file_with_result(conn, file_path):
    with open(file_path, "r") as f:
        query = f.read()

    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()

    colnames = [desc[0] for desc in cur.description] if cur.description else None
    cur.close()

    return pl.DataFrame(results, schema=colnames, orient="row")


def run_q1(conn) -> pl.DataFrame:
    return _run_sql_file_with_result(conn, "src/q1/postgres.sql")


def run_q2(conn) -> pl.DataFrame:
    return _run_sql_file_with_result(conn, "src/q2/postgres.sql")


def run_q3(conn) -> pl.DataFrame:
    return _run_sql_file_with_result(conn, "src/q3/postgres.sql")


def run_q4(conn, category: str = "Restaurant") -> pl.DataFrame:
    with open("src/q4/postgres.sql", "r") as f:
        query = f.read()

    if category == "Others":
        query = """
        SELECT COUNT(*) as venue_count
        FROM pois
        WHERE NOT (
            to_tsvector('english', category) @@ to_tsquery('english', 'Restaurant') OR
            to_tsvector('english', category) @@ to_tsquery('english', 'Club') OR
            to_tsvector('english', category) @@ to_tsquery('english', 'Museum') OR
            to_tsvector('english', category) @@ to_tsquery('english', 'Shop')
        );
        """
    else:
        query = query.format(category=category)

    cur = conn.cursor()
    cur.execute(query)
    results = cur.fetchall()

    colnames = [desc[0] for desc in cur.description] if cur.description else None
    cur.close()

    df = pl.DataFrame(results, schema=colnames, orient="row")
    return df.with_columns(pl.lit(category).alias("custom_cat"))
