CREATE TABLE IF NOT EXISTS papers (
    -- Identity & bibliographic
    openalex_id                 TEXT        PRIMARY KEY,
    doi                         TEXT,
    title                       TEXT        NOT NULL,
    publication_date            DATE,
    publication_year            SMALLINT,
    language                    CHAR(5),
    type                        TEXT,

    -- Journal / venue
    journal_name                TEXT,
    journal_issn                TEXT,

    -- Open access
    is_oa                       BOOLEAN,
    oa_status                   TEXT,

    -- Citation metrics
    cited_by_count              INTEGER,
    fwci                        REAL,
    citation_percentile         REAL,
    is_in_top_1_percent         BOOLEAN,
    is_in_top_10_percent        BOOLEAN,
    referenced_works_count      INTEGER,

    -- Collaboration scope
    author_count                SMALLINT,
    countries_distinct_count    SMALLINT,
    institutions_distinct_count SMALLINT,

    -- Primary topic / classification
    primary_topic               TEXT,
    primary_subfield            TEXT,
    primary_field               TEXT,

    -- First author
    first_author_name           TEXT,
    first_author_id             TEXT,

    -- Quality flags
    is_retracted                BOOLEAN,

    -- Pipeline metadata
    ingested_at                 TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_papers_publication_date ON papers (publication_date);
CREATE INDEX IF NOT EXISTS idx_papers_primary_field    ON papers (primary_field);
CREATE INDEX IF NOT EXISTS idx_papers_cited_by_count   ON papers (cited_by_count DESC);
