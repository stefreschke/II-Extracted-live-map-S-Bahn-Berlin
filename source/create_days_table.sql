CREATE TABLE IF NOT EXISTS "days" (
                "day" TEXT PRIMARY KEY,
                "vehicle" TEXT NOT NULL,
                "first_seen" TEXT NOT NULL,
                "last_seen" TEXT NOT NULL,
                "delay" INTEGER NOT NULL
            );