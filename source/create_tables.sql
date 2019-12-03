CREATE TABLE "snapshot" (
    "id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "taken_at"	TEXT NOT NULL
);
CREATE TABLE "datarecord" (
    "id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "snapshot_id"	INTEGER NOT NULL,
    "x"	INTEGER NOT NULL,
    "y"	INTEGER NOT NULL,
    "n"	TEXT NOT NULL,
    "l"	TEXT NOT NULL,
    "i"	TEXT NOT NULL,
    "rt"	INTEGER NOT NULL,
    "rd"	TEXT NOT NULL,
    "d"	INTEGER NOT NULL,
    "c"	INTEGER NOT NULL,
    FOREIGN KEY(snapshot_id) REFERENCES snapshot(id)
);
CREATE TABLE "projection" (
    "id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "datarecord_id"	INTEGER NOT NULL,
    "x"	INTEGER NOT NULL,
    "y"	INTEGER NOT NULL,
    "t"	INTEGER NOT NULL,
    "d"	INTEGER,
    FOREIGN KEY(datarecord_id) REFERENCES datarecord(id)
);

CREATE TABLE "weather" (
    "id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
    "at_time"	TEXT NOT NULL UNIQUE,
    "temperature" REAL NOT NULL,
    "condition" TEXT NOT NULL
);