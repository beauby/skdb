import sqlite3
from flask import Flask, g, json

DATABASE = 'database.db'

app = Flask(__name__)

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db

def init_db():
    with app.app_context():
        db = get_db()
        with app.open_resource('schema.sql', mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

@app.get("/posts")
def posts_index():
    db = get_db()
    cur = db.execute(
        "SELECT *, (SELECT COUNT(id) FROM likes WHERE post_id=posts.id) FROM posts"
    )
    res = cur.fetchall()
    cur.close()
    
    return json.jsonify(res)

@app.post("/posts/<int:post_id>/likes")
def like_post(post_id):
    db = get_db()
    db.execute(f"INSERT INTO likes(post_id) VALUES({post_id})")
    db.commit()

    return "ok", 200

@app.get("/posts/<int:post_id>/likes")
def post_likes_index(post_id):
    db = get_db()
    cur = db.execute(f"SELECT COUNT(id) FROM likes WHERE post_id={post_id}")
    res = cur.fetchall()[0][0]
    cur.close()

    return json.jsonify(res)
