CREATE TABLE IF NOT EXISTS posts (id INTEGER PRIMARY KEY AUTOINCREMENT, `title` TEXT, `body` TEXT);
INSERT INTO posts VALUES (1, 'Hello!', 'This is a post'), (2, 'Reactive stuff', 'This is neat.');
CREATE TABLE IF NOT EXISTS likes (id INTEGER PRIMARY KEY AUTOINCREMENT, post_id INTEGER);
