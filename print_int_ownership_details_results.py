import duckdb
from tabulate import tabulate

# Path to DuckDB database
db_file = "dev.duckdb"

# SQL query
analysis_query = """
CREATE OR REPLACE TABLE int_ownership_details AS (
-- Step 1: Extract details with entity type of 'user'
WITH user_details AS (
    SELECT
        user_entity.urn AS owner_urn,
        JSON_EXTRACT_STRING(user_entity.entity_details, '$.username') AS username,
        JSON_EXTRACT_STRING(user_entity.entity_details, '$.title') AS title
    FROM
        stg_datahub_entities AS user_entity
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
        cleaned_entity_owners
),

-- Step 3: Utilize normalized entity domain model to capture domain assignments
entity_domains AS (
    SELECT
        entity_urn,
        domain_name
    FROM
        cleaned_entity_domains
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
    entity_owners.entity_urn = entity_domains.entity_urn);
"""

# Connect to DuckDB
con = duckdb.connect(db_file)

# Run the query to create the table
print("Creating the table...")
con.execute(analysis_query)
print("Table 'int_ownership_details' created successfully.")

# Verify the table by querying the first few rows
results = con.execute("SELECT * FROM int_ownership_details LIMIT 5;").fetchall()
columns = [desc[0] for desc in con.description]

# Display results in tabular format
if results:
    print("\nPreview of the created table:")
    print(tabulate(results, headers=columns, tablefmt="grid"))
else:
    print("Table is empty.")

# Close the connection
con.close()