SELECT COALESCE(COUNT(*),0) count_row
FROM {master_customer_table}
WHERE customer_id = '{customer_id}'
