CREATE TABLE IF NOT EXISTS imdb_title_basics (
    tconst TEXT PRIMARY KEY,
    titleType TEXT,
    primaryTitle TEXT,
    originalTitle TEXT,
    isAdult TEXT,
    startYear INT,
    endYear INT,
    runtimeMinutes INT,
    genres TEXT
);
