/* 

Questions to Answer:

- Which Glossary Terms have been assigned to datasets and/or dashboards?
- How many datasets and/or dashboards have they been assigned to?

*/ 

with urns_with_terms as (
    select
      json_extract_string(term.value, '$.urn') as term_urn
      , count(*) as dashboard_dataset_count --aggregation higher in run order for performance
    from
      stg_datahub_entities
      , unnest(json_extract(glossary_terms, '$.terms')::json[]) as term(value)
    where
      glossary_terms is not null
      and entity_type in ('dashboard', 'dataset') --filters latent case where domains are assigned to other entity types
        
    group by all
)

select
  urns_with_terms.term_urn 
  , json_extract_string(stg_datahub_entities.entity_details, '$.name') as term_name
  , urns_with_terms.dashboard_dataset_count

from
  urns_with_terms
left join
  stg_datahub_entities
  on stg_datahub_entities.urn = urns_with_terms.term_urn

order by urns_with_terms.dashboard_dataset_count desc
;

/*

Query Output:

┌──────────────────────────────────────────────────────────┬───────────────────────┬─────────────────────────┐
│                        term_urn                          │       term_name       │ dashboard_dataset_count │
│                         varchar                          │        varchar        │         int64           │
├──────────────────────────────────────────────────────────┼───────────────────────┤─────────────────────────┤
│ urn:li:glossaryTerm:9afa9a59-93b2-47cb-9094-aa342eec24ad │ Gold Tier             │                     668 │
│ urn:li:glossaryTerm:Classification.Confidential          │ Confidential          │                      60 │
│ urn:li:glossaryTerm:Ecommerce.ReturnRate                 │ Return Rate           │                       9 │
│ urn:li:glossaryTerm:Adoption.ReturnRate                  │ Return Rate           │                       7 │
│ urn:li:glossaryTerm:c10049f8-64dc-49d5-bc25-fd1d953fac05 │ Certification Pending │                       1 │
└──────────────────────────────────────────────────────────┴───────────────────────┴─────────────────────────┘

*/