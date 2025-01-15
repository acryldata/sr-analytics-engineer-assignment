/*

Questions to Answer:

- Who has been assigned as owners to dashboards and/or datasets?
- How many dashboards and/or datasets do they own?
- What is their job title?

*/

/*
Initial code refactoring comments
1. Remove commented out code not being used for outputting query results. Cleaner code = better code.
2. Breaking out the query into additiional CTEs for better debugging and compartmentalizing code in digestible chunks.
3. Add upper cases to syntax where applicable
*/

-- Step 1: Extract owner URNs from datasets and dashboards
with entity_owners AS (
    SELECT
        entities.urn AS entity_urn,
        entities.entity_type,
        json_extract_string(owner.value, '$.owner') AS owner_urn
    FROM
        -- Rename alias from a to entities for readability
        stg_datahub_entities AS entities,
        -- Use existing unnest function to take the flattened JSON array of owners and convert into separate rows
        unnest(json_extract(owners, '$.owners')::json[]) AS owner(value)
    WHERE
        -- Limit query to datasets and dashboards only
        entities.entity_type IN ('dataset', 'dashboard')
        AND owners IS NOT NULL
),

-- Step 2: Get owner details
owner_details AS (
    SELECT
        entities.urn as owner_urn,
        -- Update alias from username to user_name for readability. Debating between updating user_name to owner_name instead, but went with user_name. A PR review would suggest alternatives.
        json_extract_string(entities.entity_details, '$.username') AS user_name,
        -- Update alias from title to owner_title for readability
        json_extract_string(entities.entity_details, '$.title') AS owner_title
    FROM
        -- Rename alias from b to entities for readability
        stg_datahub_entities entities
    WHERE
        -- Limit to user entities
        entities.entity_type = 'user'
),

-- Step 3: Combine owner type and details and calculate ownership counts
ownership_summary AS (
    SELECT
        entity_owners.entity_type,
        owner_details.user_name,
        owner_details.owner_title,
        count(distinct entity_owners.entity_urn) AS entity_count
    FROM
        entity_owners
    LEFT JOIN
        owner_details
    ON
        entity_owners.owner_urn = owner_details.owner_urn
    GROUP BY
    -- Remove numbered column in group by and explicity listing column names instead.
        entity_owners.entity_type, 
        owner_details.user_name, 
        owner_details.owner_title
)

-- Step 4: Who owns what, their title, and the count of owned entities
SELECT
-- Change order of columns to be more readable for end user
    user_name,
    owner_title,
    entity_type,
    entity_count
FROM
    ownership_summary
ORDER BY
-- Explicitly specify column name instead of the ordered number
    entity_count DESC, 
    user_name, 
    entity_type;


/*

Query Output:

┌─────────────┬───────────────────────┬─────────────────────────┬───────┐
│ entity_type │       username        │          title          │  cnt  │
│   varchar   │        varchar        │         varchar         │ int64 │
├─────────────┼───────────────────────┼─────────────────────────┼───────┤
│ dataset     │ chris@longtail.com    │ Data Engineer           │   218 │
│ dataset     │ eddie@longtail.com    │ Analyst                 │   360 │
│ dataset     │ melina@longtail.com   │ Analyst                 │    24 │
│ dashboard   │ mitch@longtail.com    │ Software Engineer       │    21 │
│ dataset     │ mitch@longtail.com    │ Software Engineer       │    97 │
│ dataset     │ phillipe@longtail.com │ Fulfillment Coordinator │    96 │
│ dataset     │ roselia@longtail.com  │ Analyst                 │    73 │
│ dataset     │ shannon@longtail.com  │ Analytics Engineer      │   300 │
│ dataset     │ terrance@longtail.com │ Fulfillment Coordinator │    32 │
└─────────────┴───────────────────────┴─────────────────────────┴───────┘

*/