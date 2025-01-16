/*

Questions to Answer:

- Which Domain is most commonly applied to datasets and/or dashboards?
- How many datasets and/or dashboards is that Domain applied to?
- What is the description of that Domain?

*/

with entity_with_domains as ( --separated into CTEs for readability
	select
	  trim(both '"' from domain_flat.domain_urn) as domain_urn
	  , entity_with_domains.urn as entity_urn
	  , entity_type
	from
	  stg_datahub_entities as entity_with_domains
	  , unnest(json_extract_string(entity_with_domains.domains, '$.domains')::string[]) as domain_flat(domain_urn)
	  
	  where entity_with_domains.entity_type in ('dataset', 'dashboard') --filters high in run order and filters latent case where domains are assigned to other entity types
)

, entity_w_domain_det as (
	select
	  entity_with_domains.domain_urn
	  , json_extract_string(domain_details.entity_details, '$.name') as domain_name
	  , json_extract_string(domain_details.entity_details, '$.description') as domain_description
	  , count(distinct entity_with_domains.entity_urn) as dashboard_dataset_count
	from entity_with_domains
	left join
	  stg_datahub_entities as domain_details
	  on domain_urn = domain_details.urn  

	group by all -- group by all is simpler than 1, 2, 3 etc.
)

select
  domain_name
  , domain_description
  , dashboard_dataset_count --made name explicit

from entity_w_domain_det

qualify rank() over (order by dashboard_dataset_count desc) = 1 --using qualify rank so ties show up instead of creating a nondeteministic query

-- order by was ordering by description, not count

/*

Query Output:

┌─────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─────────────────────────┐
│ domain_name │                                       domain_description                                                           │ dashboard_dataset_count │
│   varchar   │                                            varchar                                                                 │    int64                │
├─────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────┤
│    Finance  | All data entities required for the Finance team to generate and maintain revenue forecasts and relevant reporting. |    285                  │ 
└─────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────────────┘

*/