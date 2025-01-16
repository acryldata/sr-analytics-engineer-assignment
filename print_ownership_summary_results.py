import duckdb
from tabulate import tabulate

# Path to DuckDB database
db_file = "dev.duckdb"

# SQL query
analysis_query = """
CREATE OR REPLACE TABLE ownership_summary AS (
WITH ownership_details AS (
    SELECT
        entity_urn,
        entity_type,
        owner_username,
        owner_title,
        associated_domain
    FROM
       int_ownership_details
)

SELECT
    owner_username,
    owner_title,
    associated_domain,
    entity_type,
    COUNT(DISTINCT entity_urn) AS entity_count
FROM
    ownership_details
GROUP BY
    owner_username, 
    owner_title, 
    associated_domain, 
    entity_type
ORDER BY
    entity_count DESC, 
    owner_username, 
    associated_domain, 
    entity_type);
"""

# Connect to DuckDB
con = duckdb.connect(db_file)

# Run the query to create the table
print("Creating the table...")
con.execute(analysis_query)
print("Table 'ownership_summary' created successfully.")

# Verify the table by querying the first few rows
results = con.execute("SELECT * FROM ownership_summary order by 1 desc;").fetchall()
columns = [desc[0] for desc in con.description]

# Display results in tabular format
if results:
    print("\nPreview of the created table:")
    print(tabulate(results, headers=columns, tablefmt="grid"))
else:
    print("Table is empty.")

# Close the connection
con.close()