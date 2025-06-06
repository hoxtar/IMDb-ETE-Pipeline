CREATE TABLE IF NOT EXISTS imdb_title_akas (
    titleId TEXT NOT NULL,
    ordering INT NOT NULL,
    title TEXT,
    region TEXT,
    language TEXT,
    types TEXT,        -- Array of text (PostgreSQL syntax)
    attributes TEXT,   -- Array of text (PostgreSQL syntax)
    isOriginalTitle BOOLEAN,
    PRIMARY KEY (titleId, ordering)
);
