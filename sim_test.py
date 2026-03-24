from row_count_audittrail import compare_number

# Test comparison to ensure our refactor of table names works correctly.
bq_list = [{"table_name": "tree_db_users", "row_count": 500}]
oci_list = [{"table_name": "users", "row_count": 650}] # Diff 150 > 100

res = compare_number(bq_list, oci_list, "test_db", [])
print(res)
