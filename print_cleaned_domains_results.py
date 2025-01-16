import duckdb
from tabulate import tabulate

# Path to DuckDB database
db_file = "dev.duckdb"

# SQL query
analysis_query = """
CREATE OR REPLACE TABLE cleaned_entity_domains AS (
-- Step 1: Parse out owner level unique identifier
WITH parsed_domains AS (
    SELECT
        entity_with_domains.urn AS entity_urn,
        JSON_EXTRACT_STRING(domain_flat.domain_urn, '$') AS domain_urn
    FROM
        stg_datahub_entities AS entity_with_domains,
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
        stg_datahub_entities
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
    parsed_domains.domain_urn = domain_details.domain_urn);
"""

# Connect to DuckDB
con = duckdb.connect(db_file)

# Run the query to create the table
print("Creating the table...")
con.execute(analysis_query)
print("Table 'cleaned_entity_domains' created successfully.")

# Verify the table by querying the first few rows
results = con.execute("SELECT * FROM cleaned_entity_domains LIMIT 5;").fetchall()
columns = [desc[0] for desc in con.description]

# Display results in tabular format
if results:
    print("\nPreview of the created table:")
    print(tabulate(results, headers=columns, tablefmt="grid"))
else:
    print("Table is empty.")

# Close the connection
con.close()