CREATE TABLE IF NOT EXISTS imdb_title_crew (
    tconst TEXT PRIMARY KEY,
    directors TEXT[],
    writers TEXT[]
);
