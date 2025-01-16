/* 

Questions to Answer:

- Which Glossary Terms have been assigned to datasets and/or dashboards?
- How many datasets and/or dashboards have they been assigned to?

*/ 

/*
Initial code refactoring comments
1. Remove any references to commented out code not being used for outputting query results. Cleaner code = better code.
2. Breaking out the query into CTEs for better debugging and compartmentalizing code in digestible chunks
3. Adding formatting and new lines for better readability
4. Add upper cases to syntax where applicable
*/

-- Step 1: Extracting URNs of glossary terms. 
WITH urns_with_terms AS (
    SELECT
        urn,
        JSON_EXTRACT_STRING(term.value, '$.urn') AS term_urn
    FROM
        stg_datahub_entities,
-- Replaced Cross Join with implicit join. Cross join is explicitly taking every row from stg table and joining it with unnested JSON array from glossary_terms column. Easier to read and debug personally
        UNNEST(JSON_EXTRACT(glossary_terms, '$.terms')::JSON[]) AS term(value)
    WHERE
        glossary_terms IS NOT NULL
),

-- Step 2: Filter columns with values of 'datasets' and dashboards'
entity_glossary_assignments AS (
    SELECT
        entities.urn AS entity_urn,
        entities.entity_type,
        JSON_EXTRACT_STRING(term.value, '$.urn') AS term_urn
    FROM
        stg_datahub_entities AS entities,
        UNNEST(JSON_EXTRACT(entities.glossary_terms, '$.terms')::JSON[]) AS term(value)
    WHERE
        entities.glossary_terms IS NOT NULL
        AND entities.entity_type IN ('dataset', 'dashboard')
),

-- Step 3: Extract metadata from glossary terms
glossary_term_counts AS (
    SELECT
        term_urn,
        JSON_EXTRACT_STRING(stg_datahub_entities.entity_details, '$.name') AS term_name,
        COUNT(DISTINCT entity_glossary_assignments.entity_urn) AS urn_count
    FROM
        entity_glossary_assignments
    LEFT JOIN
        stg_datahub_entities
        ON stg_datahub_entities.urn = entity_glossary_assignments.term_urn
    GROUP BY
        term_urn, term_name
)

-- Step 4: Final query to generate term name and total counts of URN
SELECT
    term_name,
    SUM(urn_count) AS total_urn_count
FROM
    glossary_term_counts
GROUP BY
-- Remove numbered column in group by and explicity listing column names instead. It allows for ease of maintenance and allows any other reviewer to understand the query easily
    term_name
ORDER BY
-- Explicitly specify column name instead of the ordered number
    total_urn_count DESC;

/*

Query Output:

┌───────────────────────┬───────────┐
│       term_name       │ urn_count │
│        varchar        │   int64   │
├───────────────────────┼───────────┤
│ Gold Tier             │       668 │
│ Confidential          │        60 │
│ Return Rate           │        16 │
│ Certification Pending │         1 │
└───────────────────────┴───────────┘

*/