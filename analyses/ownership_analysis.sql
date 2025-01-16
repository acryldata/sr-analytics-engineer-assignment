/*

Questions to Answer:

- Who has been assigned as owners to dashboards and/or datasets?
- How many dashboards and/or datasets do they own?
- What is their job title?

*/

with users as ( --doing this in CTE for readability
	select
	  urn
	  , json_extract_string(entity_details, '$.fullName') full_name -- (minor & audience specific) "who" implies their name
	  , json_extract_string(entity_details, '$.username') user_name
      , json_extract_string(entity_details, '$.title') as title 
      
	from 
	  stg_datahub_entities
	where 
	  entity_type in ('user')
)

select 
  a.entity_type --this adds a row for mitch - OK to leave in depending on audience
  , users.full_name
  , users.user_name
  , users.title
  , count(*) dataset_dashboard_count
--  , count(distinct a.urn) as cnt
from stg_datahub_entities as a  
  , unnest(json_extract(owners, '$.owners')::json[]) as term(owner_urn)
left outer join
  users
  on json_extract_string(owner_urn, '$.owner') = users.urn

where entity_type in ('dashboard', 'dataset') --explicit about entity types

group by all

order by full_name, entity_type desc


/*

Query Output:

┌─────────────┬───────────────────────┬───────────────────────┬─────────────────────────┬───────┐
│ entity_type │       username        |      username         │          title          │  cnt  │
│   varchar   │        varchar        |       varchar         │         varchar         │ int64 │
├─────────────┼───────────────────────┼───────────────────────┼─────────────────────────┼───────┤
│ dataset     │ Chris Ewing           | chris@longtail.com    │ Data Engineer           │   218 │
│ dataset     │ Eddie Winton          | eddie@longtail.com    │ Analyst                 │   360 │
│ dataset     │ Melina Eliez          | melina@longtail.com   │ Analyst                 │    24 │
│ dashboard   │ Mitch Terzi           | mitch@longtail.com    │ Software Engineer       │    21 │
│ dataset     │ Mitch Terzi           | mitch@longtail.com    │ Software Engineer       │    97 │
│ dataset     │ Phillipe Fissenden    | phillipe@longtail.com │ Fulfillment Coordinator │    96 │
│ dataset     │ Roselia Himsworth     | roselia@longtail.com  │ Analyst                 │    73 │
│ dataset     │ Shannon Lovett        | shannon@longtail.com  │ Analytics Engineer      │   300 │
│ dataset     │ Terrance Gude         | terrance@longtail.com │ Fulfillment Coordinator │    32 │
└─────────────┴───────────────────────┴───────────────────────┴─────────────────────────┴───────┘

*/