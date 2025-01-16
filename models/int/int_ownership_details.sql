{{ config(
    materialized='table'
) }}

-- Step 1: Extract details with entity type of 'user'
WITH user_details AS (
    SELECT
        user_entity.urn AS owner_urn,
        JSON_EXTRACT_STRING(user_entity.entity_details, '$.username') AS username,
        JSON_EXTRACT_STRING(user_entity.entity_details, '$.title') AS title
    FROM
        {{ ref('stg_datahub_entities') }} AS user_entity
    WHERE
        user_entity.entity_type = 'user'
),

-- Step 2: Utilize normalized entity owners model to capture owner relationships
entity_owners AS (
    SELECT
        entity_urn,
        entity_type,
        owner_urn
    FROM
        {{ ref('cleaned_entity_owners') }}
),

-- Step 3: Utilize normalized entity domain model to capture domain assignments
entity_domains AS (
    SELECT
        entity_urn,
        domain_name
    FROM
        {{ ref('cleaned_entity_domains') }}
)

-- Step 4: Join entity, user and domain models to conduct exploratory analysis
SELECT
    entity_owners.entity_urn,
    entity_owners.entity_type,
    user_details.username AS owner_username,
    user_details.title AS owner_title,
    entity_domains.domain_name AS associated_domain
FROM
    entity_owners
LEFT JOIN
    user_details
ON
    entity_owners.owner_urn = user_details.owner_urn
LEFT JOIN
    entity_domains
ON
    entity_owners.entity_urn = entity_domains.entity_urn;
