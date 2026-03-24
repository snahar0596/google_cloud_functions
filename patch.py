with open('row_count_audittrail.py', 'r') as f:
    lines = f.readlines()

out = lines[:16] + [
    "INTEGRATION_TABLE_NAME = 'ctrl_dataset.integration_layer_tables'\n",
    "CORESYSTEM_TABLE_NAME  = 'ctrl_dataset.coresystem_tables'\n",
    "\n",
    "def get_all_table_metadata(ctrl_table):\n",
    "    \"\"\"\n",
    "    Fetches the primary keys for all tables defined in the control table\n",
    "    in a single BigQuery call. Returns a dictionary mapping table_name to pk_col.\n",
    "    \"\"\"\n",
    "    query = f\"SELECT table_name, pk_col FROM {ctrl_table}\"\n",
    "    client = bigquery.Client()\n",
    "    query_job = client.query(query)\n",
    "    \n",
    "    metadata = {}\n",
    "    for row in query_job.result():\n",
    "        if row.table_name:\n",
    "            metadata[row.table_name] = row.pk_col\n",
    "    return metadata\n",
    "\n"
] + lines[73:]

with open('row_count_audittrail.py', 'w') as f:
    f.writelines(out)
