/*

Questions to Answer:

- Which Domain is most commonly applied to datasets and/or dashboards?
- How many datasets and/or dashboards is that Domain applied to?
- What is the description of that Domain?

*/

/*
Initial code refactoring comments
1. Remove commented out code not being used for outputting query results. Cleaner code = better code.
2. Breaking out the query into CTEs for better debugging and understanding of code
*/

-- Step 1: Parse the JSON domain values into individual rows. 
with parsed_domains as 
(
  select
    entity_with_domains.urn as entity_urn,
    json_extract_string(domain_flat.domain_urn, '$') as domain_urn
  from
    stg_datahub_entities as entity_with_domains,
    -- Use existing unnest function to take the flattened JSON array of domains and convert into separate rows
    unnest(json_extract_string(entity_with_domains.domains, '$.domains')::string[]) as domain_flat(domain_urn)
  where
    entity_with_domains.domains is not null
),

-- Step 2: Add domain metadata for corresponding domain_urn
domain_details as 
(
  select
    urn as domain_urn,
    json_extract_string(entity_details, '$.name') as domain_name,
    json_extract_string(entity_details, '$.description') as domain_description
  from
    stg_datahub_entities
)

-- Step 3: Aggregate results
select
  d.domain_name,
  d.domain_description,
-- Update column name from entity_count to domain_entity_count for clearer understanding of columns
  count(distinct pd.entity_urn) as domain_entity_count
from
  parsed_domains as pd
left join
  domain_details as d
  on pd.domain_urn = d.domain_urn
group by
-- Remove numbered columns in group by and explicity listing column names instead. It allows for ease of maintenance and allows any other reviewer to understand the query easily
  d.domain_name, 
  d.domain_description
order by
-- Ordering by entity_count helps answer how many datasets and/or dashboards is that Domain applied to
  domain_entity_count desc
limit 1;

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