# IMDb Table Schemas

This document provides a detailed description of each table schema used in the IMDb End-to-End Data Pipeline.

## Table: `imdb_title_akas`
| Column            | Type    | Description                                                                 |
|-------------------|---------|-----------------------------------------------------------------------------|
| titleId           | TEXT    | Alphanumeric unique identifier of the title (tconst)                       |
| ordering          | INTEGER | Unique number for rows per titleId                                         |
| title             | TEXT    | Localized title                                                            |
| region            | TEXT    | Region for this version of the title                                       |
| language          | TEXT    | Language of the title                                                      |
| types             | TEXT[]  | Attributes: "alternative", "dvd", "festival", etc.                        |
| attributes        | TEXT[]  | Additional terms for this alternative title                                |
| isOriginalTitle   | BOOLEAN | 1: original title, 0: not original title                                   |

---

## Table: `imdb_title_basics`
| Column           | Type    | Description                                                                 |
|------------------|---------|-----------------------------------------------------------------------------|
| tconst           | TEXT    | Unique identifier of the title                                             |
| titleType        | TEXT    | Type/format (e.g., movie, short, tvseries)                                  |
| primaryTitle     | TEXT    | Popular or promotional title                                               |
| originalTitle    | TEXT    | Title in original language                                                 |
| isAdult          | BOOLEAN | 1: adult content, 0: non-adult                                             |
| startYear        | INTEGER | Release year                                                               |
| endYear          | INTEGER | End year for TV series, NULL for others                                    |
| runtimeMinutes   | INTEGER | Runtime in minutes                                                         |
| genres           | TEXT[]  | Up to 3 genres associated with the title                                   |

---

## Table: `imdb_title_crew`
| Column    | Type    | Description                                         |
|-----------|---------|-----------------------------------------------------|
| tconst    | TEXT    | Unique identifier of the title                     |
| directors | TEXT[]  | List of nconst IDs of directors                    |
| writers   | TEXT[]  | List of nconst IDs of writers                      |

---

## Table: `imdb_title_episode`
| Column        | Type    | Description                                     |
|---------------|---------|-------------------------------------------------|
| tconst        | TEXT    | Identifier of episode                          |
| parentTconst  | TEXT    | Identifier of parent TV series                 |
| seasonNumber  | INTEGER | Season number                                   |
| episodeNumber | INTEGER | Episode number in the season                   |

---

## Table: `imdb_title_principals`
| Column    | Type    | Description                                         |
|-----------|---------|-----------------------------------------------------|
| tconst    | TEXT    | Unique identifier of the title                    |
| ordering  | INTEGER | Unique number for rows per titleId               |
| nconst    | TEXT    | Unique identifier of the name/person             |
| category  | TEXT    | Job category                                      |
| job       | TEXT    | Specific job title or NULL                       |
| characters| TEXT    | Character name or NULL                           |

---

## Table: `imdb_title_ratings`
| Column        | Type    | Description                                      |
|---------------|---------|--------------------------------------------------|
| tconst        | TEXT    | Unique identifier of the title                  |
| averageRating | FLOAT   | Weighted average of user ratings                |
| numVotes      | INTEGER | Number of votes received                        |

---

## Table: `imdb_name_basics`
| Column            | Type    | Description                                      |
|-------------------|---------|--------------------------------------------------|
| nconst            | TEXT    | Unique identifier of the person                 |
| primaryName       | TEXT    | Most credited name                              |
| birthYear         | INTEGER | Birth year in YYYY format                       |
| deathYear         | INTEGER | Death year in YYYY format or NULL              |
| primaryProfession | TEXT[]  | Top-3 professions                               |
| knownForTitles    | TEXT[]  | Titles the person is known for                 |

---

## Notes:
- All files are **gzipped TSV (tab-separated values)**.
- The first line in each file is a **header row**.
- A **'\N'** denotes missing or NULL values.

---

## Future Plan
These schemas will serve as a base for **dbt model creation**, enabling:
- **Auto-generated documentation** via `dbt docs generate`
- Schema evolution tracking
- **Data lineage** visualization
s
