/*

Questions to Answer:

- Which Domain is most commonly applied to datasets and/or dashboards?
- How many datasets and/or dashboards is that Domain applied to?
- What is the description of that Domain?

*/

/*
Initial code refactoring comments
1. Remove commented out code not being used for outputting query results. Cleaner code = better code.
2. Breaking out the query into CTEs for better debugging and compartmentalizing code in digestible chunks
3. Add upper cases to syntax where applicable
*/

-- Step 1: Parse the JSON domain values into individual rows. 
WITH parsed_domains AS 
(
  SELECT
    entity_with_domains.urn AS entity_urn,
    json_extract_string(domain_flat.domain_urn, '$') AS domain_urn
  FROM
    stg_datahub_entities AS entity_with_domains,
    -- Use existing unnest function to take the flattened JSON array of domains and convert into separate rows
    unnest(json_extract_string(entity_with_domains.domains, '$.domains')::string[]) AS domain_flat(domain_urn)
  WHERE
    entity_with_domains.domains IS NOT NULL
),

-- Step 2: Add domain metadata for corresponding domain_urn
domain_details AS 
(
  SELECT
    urn AS domain_urn,
    json_extract_string(entity_details, '$.name') AS domain_name,
    json_extract_string(entity_details, '$.description') AS domain_description
  FROM
    stg_datahub_entities
)

-- Step 3: Aggregate results
SELECT
  d.domain_name,
  d.domain_description,
-- Update column name from entity_count to domain_entity_count for clearer understanding of columns
  count(distinct pd.entity_urn) AS domain_entity_count
FROM
  parsed_domains AS pd
LEFT JOIN
  domain_details AS d
  on pd.domain_urn = d.domain_urn
GROUP BY
-- Remove numbered columns in group by and explicity listing column names instead. It allows for ease of maintenance and allows any other reviewer to understand the query easily
  d.domain_name, 
  d.domain_description
ORDER BY
-- Ordering by entity_count helps answer how many datasets and/or dashboards is that Domain applied to
  domain_entity_count DESC
LIMIT 1;

/*
New Query Output:

┌─────────────┬────────────────────────────────────────────────────────────────────────────────────────────────┬─────────────────────┐
│ domain_name │                                       domain_description                                       │ domain_entity_count │
│   varchar   │                                            varchar                                             │    int64            │
├─────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┼─────────────────────┤
│ Finance     │ All data entities required for the Finance team to generate and maintain revenue forecasts  …  │        285          │
└─────────────┴────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────────┘

Query Output:

┌─────────────┬────────────────────────────────────────────────────────────────────────────────────────────────┬──────────────┐
│ domain_name │                                       domain_description                                       │ entity_count │
│   varchar   │                                            varchar                                             │    int64     │
├─────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┼──────────────┤
│ E-Commerce  │ The E-Commerce Data Domain within Datahub provides access to datasets related to online reta…  │           65 │
└─────────────┴────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┘

*/