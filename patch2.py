with open('row_count_audittrail.py', 'r') as f:
    lines = f.readlines()

def_idx = -1
for i, line in enumerate(lines):
    if line.startswith('def get_count_query'):
        def_idx = i
        break

compare_idx = -1
for i, line in enumerate(lines):
    if line.startswith('def compare_number'):
        compare_idx = i
        break

new_get_count_query = """def get_count_query(db_bq, db_oci, table, metadata):
    client = bigquery.Client()

    query = f\"\"\"
        SELECT table_name
        FROM {table}
        where table_name != 'tree_db_policy'
    \"\"\"
    query_job = client.query(query)
    rows = query_job.result()

    query_parts_bq = []
    queries_oci = []

    for row in rows:
        table_name = row.table_name
        pk = metadata.get(table_name)
        if not pk:
            continue

        # BQ Query part for UNION ALL
        query_bq = f\"\"\"
            SELECT '{table_name}' AS table_name, COUNT(distinct payload.{pk}) AS row_count
            FROM {db_bq}.{table_name}
        \"\"\"
        query_parts_bq.append(query_bq.strip())

        # OCI Query (individual)
        oci_table_name = table_name.replace("tree_db_", "").replace("tree_integration_", "")
        query_oci = f"SELECT '{table_name}' AS table_name, COUNT({pk}) AS row_count FROM {db_oci}.{oci_table_name}"
        queries_oci.append(query_oci)

    query_bq_final = "\\nUNION ALL\\n".join(query_parts_bq)

    return query_bq_final, queries_oci

"""

new_concurrent_oci = """
import concurrent.futures
import threading

# Thread-local storage for DB connections
thread_local = threading.local()

def get_db_connection(db_config):
    if not hasattr(thread_local, "connection") or not thread_local.connection.is_connected():
        thread_local.connection = mysql.connector.connect(**db_config)
    return thread_local.connection

def execute_single_oci_query(query, db_config):
    try:
        conn = get_db_connection(db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result
    except Exception as e:
        logging.error(f"Error executing OCI query: {query}. Error: {e}")
        return []

def get_table_counts_oci_concurrent(queries, db_config):
    results = []
    # We have 8 CPUs, let's use 16 workers to do network-bound work
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        # Submit all queries
        futures = {executor.submit(execute_single_oci_query, q, db_config): q for q in queries}
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res:
                results.extend(res)
    return results
"""

out = lines[:def_idx] + [new_get_count_query, new_concurrent_oci] + lines[compare_idx:]

with open('row_count_audittrail.py', 'w') as f:
    f.writelines(out)
