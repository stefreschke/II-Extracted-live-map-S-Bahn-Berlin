import sqlite3


def create_connection(filename="s-bahn-livekarte"):
    with sqlite3.connect('../data/' + filename) as conn:
        pass
