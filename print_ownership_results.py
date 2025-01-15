import duckdb
from tabulate import tabulate

# Path to DuckDB database and analysis query file
db_file = "dev.duckdb"
analysis_query_file = "analyses/ownership_analysis.sql"

# Connect to DuckDB
con = duckdb.connect(db_file)

# Run the analysis query
print("Running the analysis query...")
with open(analysis_query_file, "r") as f:
    analysis_query = f.read()

# Fetch results with column names
results = con.execute(analysis_query).fetchall()
columns = [desc[0] for desc in con.description]  # Extract column names

# Display results in tabular format
if results:
    print("\nResults from the analysis query:")
    print(tabulate(results, headers=columns, tablefmt="grid"))
else:
    print("No results found.")

# Close the connection
con.close()
