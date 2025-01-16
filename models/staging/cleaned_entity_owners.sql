{{ config(
    materialized='table'
) }}

-- Step 1: Parse out owner level unique identifier
WITH extracted_owners AS (
    SELECT
        entities.urn AS entity_urn,
        entities.entity_type,
        JSON_EXTRACT_STRING(owner.value, '$.owner') AS owner_urn
    FROM
        {{ ref('stg_datahub_entities') }} AS entities,
        UNNEST(JSON_EXTRACT(entities.owners, '$.owners')::JSON[]) AS owner(value)
    WHERE
        entities.entity_type IN ('dataset', 'dashboard') -- Focus on datasets/dashboards
        AND entities.owners IS NOT NULL
)

-- Step 2: Extractnormalized entity/owner level identifiers
SELECT
    entity_urn,
    entity_type,
    owner_urn
FROM
    extracted_owners
WHERE
-- Exclude rows with null owner identifiers. 
    owner_urn IS NOT NULL;