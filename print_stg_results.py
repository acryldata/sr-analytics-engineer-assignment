import duckdb
from tabulate import tabulate

# Path to DuckDB database and analysis query file
db_file = "dev.duckdb"

# Connect to DuckDB
conn = duckdb.connect(db_file)

conn.execute("""
    CREATE OR REPLACE TABLE datahub_entities_raw AS 
    SELECT * FROM read_csv('seeds/datahub_entities_raw.csv');
""")

query = """
with datasets as (
select
    datasets.urn
    , 'dataset' as entity_type
    , datasets.metadata::json as entity_details
    , datasets.createdon::timestamp as entity_created_at
    , datasets.createdby as entity_created_by
    -- add domain reference
    , domains.metadata::json as domains
    , domains.createdon::timestamp as domains_added_at
    , domains.createdby as domains_added_by
    -- add glossary term reference
    , glossary_terms.metadata::json as glossary_terms
    , glossary_terms.createdon::timestamp as glossary_terms_added_at
    , glossary_terms.createdby as glossary_terms_added_by
    -- add ownership
    , ownership.metadata::json as owners
    , ownership.createdon::timestamp as owners_added_at
    , ownership.createdby as owners_added_by
from
    datahub_entities_raw as datasets
left join
    datahub_entities_raw as domains
    on datasets.urn = domains.urn
    and domains.aspect = 'domains'
left join
    datahub_entities_raw as glossary_terms
    on datasets.urn = glossary_terms.urn
    and glossary_terms.aspect = 'glossaryTerms'
left join
    datahub_entities_raw as ownership
    on datasets.urn = ownership.urn
    and ownership.aspect = 'ownership'
where
    datasets.aspect = 'datasetKey'
)

, dashboards as (
select
    dashboards.urn
    , 'dashboard' as entity_type
    , json_merge_patch(dashboards.metadata, dashboard_details.metadata)::json as entity_details
    , dashboards.createdon::timestamp as entity_created_at
    , dashboards.createdby as entity_created_by
    -- add domain reference
    , domains.metadata::json as domains
    , domains.createdon::timestamp as domains_added_at
    , domains.createdby as domains_added_by
    -- add glossary term reference
    , glossary_terms.metadata::json as glossary_terms
    , glossary_terms.createdon::timestamp as glossary_terms_added_at
    , glossary_terms.createdby as glossary_terms_added_by
    -- add ownership
    , ownership.metadata::json as owners
    , ownership.createdon::timestamp as owners_added_at
    , ownership.createdby as owners_added_by
from
    datahub_entities_raw as dashboards
left join
    datahub_entities_raw as dashboard_details
    on dashboards.urn = dashboard_details.urn
    and dashboard_details.aspect = 'dashboardInfo'
left join
    datahub_entities_raw as domains
    on dashboards.urn = domains.urn
    and domains.aspect = 'domains'
left join
    datahub_entities_raw as glossary_terms
    on dashboards.urn = glossary_terms.urn
    and glossary_terms.aspect = 'glossaryTerms'
left join
   datahub_entities_raw as ownership
    on dashboards.urn = ownership.urn
    and ownership.aspect = 'ownership'
where
    dashboards.aspect = 'dashboardKey'
)

, domains as (
select
    domains.urn
    , 'domain' as entity_type
    , domains.metadata::json as entity_details
    , domains.createdon::timestamp as entity_created_at
    , domains.createdby as entity_created_by
    -- add domain reference
    , null::json as domains
    , null::timestamp as domains_added_at
    , null as domains_added_by
    -- add glossary term reference
    , null::json as glossary_terms
    , null::timestamp as glossary_terms_added_at
    , null as glossary_terms_added_by
    -- add ownership
    , null::json as owners
    , null::timestamp as owners_added_at
    , null as owners_added_by
from
   'datahub_entities_raw' as domains
where
    domains.aspect = 'domainProperties'
)

, glossary_terms as (
select
    glossary_terms.urn
    , 'glossary_term' as entity_type
    , glossary_terms.metadata::json as entity_details
    , glossary_terms.createdon::timestamp as entity_created_at
    , glossary_terms.createdby as entity_created_by
    -- add domain reference
    , null::json as domains
    , null::timestamp as domains_added_at
    , null as domains_added_by
    -- add glossary term reference
    , null::json as glossary_terms
    , null::timestamp as glossary_terms_added_at
    , null as glossary_terms_added_by
    -- add ownership
    , null::json as owners
    , null::timestamp as owners_added_at
    , null as owners_added_by
from
    datahub_entities_raw as glossary_terms
where
    glossary_terms.aspect = 'glossaryTermInfo'
)

, users as (
select
    users.urn
    , 'user' as entity_type
    , json_merge_patch(users.metadata, user_details.metadata)::json as entity_details
    , users.createdon::timestamp as entity_created_at
    , users.createdby as entity_created_by
    -- add domain reference
    , null::json as domains
    , null::timestamp as domains_added_at
    , null as domains_added_by
    -- add glossary term reference
    , null::json as glossary_terms
    , null::timestamp as glossary_terms_added_at
    , null as glossary_terms_added_by
    -- add ownership
    , null::json as owners
    , null::timestamp as owners_added_at
    , null as owners_added_by
from
    datahub_entities_raw as users
left join
    datahub_entities_raw as user_details
    on users.urn = user_details.urn
    and user_details.aspect = 'corpUserInfo'
where
    users.aspect = 'corpUserKey'
)

select * from datasets

union all

select * from dashboards

union all

select * from domains

union all

select * from glossary_terms

union all

select * from users
"""

# Run the query
result = conn.execute(query).fetchdf()

# Display results
print(result)