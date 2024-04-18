SELECT *
FROM
(SELECT
    customer_id,
    proba p_churn,
    created_at
FROM {full_table_name}
WHERE customer_id = '{customer_id}') temp
ORDER BY created_at DESC
LIMIT 1
