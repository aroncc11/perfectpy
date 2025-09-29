import httpx
import sqlite3
from contextlib import closing
from prefect import task, flow


# --- Extract
@task(retries=3)
def fetch_posts():
    """Obtiene posts de JSONPlaceholder"""
    resp = httpx.get("https://jsonplaceholder.cypress.io/posts", timeout=10.0)
    resp.raise_for_status()
    return resp.json()


# --- Transform
@task
def filter_long_posts(posts, min_length=120):
    """Filtra posts con body largo"""
    return [
        (p["id"], p["userId"], p["title"], p["body"])
        for p in posts
        if len(p.get("body", "")) > min_length
    ]


# --- Load
@task
def store_posts(filtered_posts):
    """Guarda los posts filtrados en SQLite"""
    create_script = '''
    CREATE TABLE IF NOT EXISTS posts (
        id INTEGER,
        userId INTEGER,
        title TEXT,
        body TEXT
    )
    '''
    insert_cmd = "INSERT INTO posts VALUES (?, ?, ?, ?)"

    with closing(sqlite3.connect("jsonposts.db")) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executescript(create_script)
            cursor.executemany(insert_cmd, filtered_posts)
            conn.commit()


# --- Flow
@flow
def jsonplaceholder_etl_flow(min_length: int = 120):
    posts = fetch_posts()
    long_posts = filter_long_posts(posts, min_length)
    store_posts(long_posts)


if __name__ == "__main__":
    jsonplaceholder_etl_flow()
