import duckdb
from tabulate import tabulate

# Path to DuckDB database
db_file = "dev.duckdb"

# SQL query
analysis_query = """
CREATE OR REPLACE TABLE cleaned_entity_owners AS (
-- Step 1: Parse out owner level unique identifier
WITH extracted_owners AS (
    SELECT
        entities.urn AS entity_urn,
        entities.entity_type,
        JSON_EXTRACT_STRING(owner.value, '$.owner') AS owner_urn
    FROM
        stg_datahub_entities AS entities,
        UNNEST(JSON_EXTRACT(entities.owners, '$.owners')::JSON[]) AS owner(value)
    WHERE
        entities.entity_type IN ('dataset', 'dashboard') -- Focus on datasets/dashboards
        AND entities.owners IS NOT NULL
)

-- Step 2: Extractnormalized entity/owner level identifiers
SELECT
    entity_urn,
    entity_type,
    owner_urn
FROM
    extracted_owners
WHERE
-- Exclude rows with null owner identifiers. 
    owner_urn IS NOT NULL);
"""

# Connect to DuckDB
con = duckdb.connect(db_file)

# Run the query to create the table
print("Creating the table...")
con.execute(analysis_query)
print("Table 'cleaned_entity_owners' created successfully.")

# Verify the table by querying the first few rows
results = con.execute("SELECT * FROM cleaned_entity_owners LIMIT 5;").fetchall()
columns = [desc[0] for desc in con.description]

# Display results in tabular format
if results:
    print("\nPreview of the created table:")
    print(tabulate(results, headers=columns, tablefmt="grid"))
else:
    print("Table is empty.")

# Close the connection
con.close()