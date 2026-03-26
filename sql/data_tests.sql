-- Test 1: NOT NULL / required fields
-- All counts must be 0.
SELECT COUNT(*) FILTER (WHERE openalex_id IS NULL)       AS missing_id,
       COUNT(*) FILTER (WHERE title IS NULL)              AS missing_title,
       COUNT(*) FILTER (WHERE publication_year IS NULL)   AS missing_year,
       COUNT(*) FILTER (WHERE type IS NULL)               AS missing_type
FROM papers;

-- Test 2: Primary key uniqueness
-- Must return 0 rows.
SELECT openalex_id, COUNT(*) AS occurrences
FROM papers
GROUP BY openalex_id
HAVING COUNT(*) > 1;

-- Test 3: Date / year consistency
-- Must return 0.
SELECT COUNT(*) AS year_mismatch
FROM papers
WHERE publication_date IS NOT NULL
  AND publication_year IS NOT NULL
  AND EXTRACT(YEAR FROM publication_date) <> publication_year;

-- Test 4: Numeric range sanity
-- All counts must be 0.
SELECT COUNT(*) FILTER (WHERE cited_by_count < 0)                      AS negative_citations,
       COUNT(*) FILTER (WHERE citation_percentile NOT BETWEEN 0 AND 1) AS bad_percentile,
       COUNT(*) FILTER (WHERE author_count < 0)                        AS bad_author_count,
       COUNT(*) FILTER (WHERE fwci < 0)                                AS negative_fwci
FROM papers;

-- Test 5: Top-1% implies top-10% consistency
-- Must return 0.
SELECT COUNT(*) AS inconsistent_top_percent_flags
FROM papers
WHERE is_in_top_1_percent  = TRUE
  AND is_in_top_10_percent = FALSE;

-- Test 6: Pipeline freshness
-- Returns 1 (fail) if no rows were ingested in the last 24 hours, 0 (pass) otherwise.
SELECT CASE
           WHEN COUNT(*) FILTER (WHERE ingested_at >= NOW() - INTERVAL '24 hours') = 0
           THEN 1 ELSE 0
       END AS stale_pipeline
FROM papers;
