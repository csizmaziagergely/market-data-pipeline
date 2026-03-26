INSERT INTO papers (
    openalex_id, doi, title, publication_date, publication_year,
    language, type,
    journal_name, journal_issn,
    is_oa, oa_status,
    cited_by_count, fwci, citation_percentile,
    is_in_top_1_percent, is_in_top_10_percent,
    referenced_works_count,
    author_count, countries_distinct_count, institutions_distinct_count,
    primary_topic, primary_subfield, primary_field,
    first_author_name, first_author_id,
    is_retracted
)
VALUES %s
ON CONFLICT (openalex_id) DO NOTHING;
