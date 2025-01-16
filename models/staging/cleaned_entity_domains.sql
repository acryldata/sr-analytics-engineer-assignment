{{ config(
    materialized='table'
) }}

-- Step 1: Parse out domain unique identifier
WITH parsed_domains AS (
    SELECT
        entity_with_domains.urn AS entity_urn,
        JSON_EXTRACT_STRING(domain_flat.domain_urn, '$') AS domain_urn
    FROM
        {{ ref('stg_datahub_entities') }} AS entity_with_domains,
        UNNEST(JSON_EXTRACT_STRING(entity_with_domains.domains, '$.domains')::STRING[]) AS domain_flat(domain_urn)
    WHERE
        entity_with_domains.domains IS NOT NULL
),

-- Step 2: Extract domain metadata
domain_details AS (
    SELECT
        urn AS domain_urn,
        JSON_EXTRACT_STRING(entity_details, '$.name') AS domain_name,
        JSON_EXTRACT_STRING(entity_details, '$.description') AS domain_description
    FROM
        {{ ref('stg_datahub_entities') }}
)

-- Step 3: Join domain details to identifier
SELECT
    parsed_domains.entity_urn,
    domain_details.domain_name,
    domain_details.domain_description
FROM
    parsed_domains AS parsed_domains
LEFT JOIN
    domain_details AS domain_details
ON
    parsed_domains.domain_urn = domain_details.domain_urn;