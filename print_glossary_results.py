import duckdb
from tabulate import tabulate
import sys

# Function to read the SQL query from a file
def read_sql_from_file(file_path):
    try:
        with open(file_path, "r") as f:
            return f.read()
    except Exception as e:
        print(f"Error reading SQL file: {e}")
        sys.exit(1)

# Path to DuckDB database and analysis query file
db_file = "dev.duckdb"

# Allow user to specify the SQL file (default to glossary_term_analysis.sql)
analysis_query_file = "analyses/glossary_term_analysis.sql" if len(sys.argv) < 2 else sys.argv[1]

# Connect to DuckDB
con = duckdb.connect(db_file)

# Run the analysis query
print(f"Running the analysis query from {analysis_query_file}...")
analysis_query = read_sql_from_file(analysis_query_file)

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
