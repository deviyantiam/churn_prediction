INSERT INTO {project_id}.{dataset}.{table_name}
WITH trx_data AS (
SELECT
    DISTINCT
    customer_id,
    date,
    SUM(1) OVER (PARTITION BY customer_id ORDER BY UNIX_DATE(date) RANGE BETWEEN 1 FOLLOWING AND 30 FOLLOWING) AS num_trx_innext30days,
FROM
    {project_id}.{dataset}.{dm_source_table}
WHERE
    date BETWEEN DATE_SUB(DATE_SUB(DATE_TRUNC(DATE_SUB(DATE('{date_input}'), INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 7 DAY), INTERVAL 5 WEEK) AND
    DATE_TRUNC(DATE_SUB(DATE('{date_input}'), INTERVAL 1 DAY), WEEK(MONDAY))
    AND
    quantity>0
GROUP BY 1,2
 ),
label_data AS (
SELECT
    customer_id,
    date,
    CASE WHEN num_trx_innext30days > 0 THEN 0 ELSE 1 END label
FROM
    trx_data
),
prediction_data AS (
SELECT
    pred.customer_id,
    pred.date,
    act.label,
    CASE WHEN pred.proba>0.5 THEN 1 ELSE 0 END p_churn
FROM
    {project_id}.{dataset}.{dm_pred_table} pred
INNER JOIN label_data act USING (customer_id,date)
WHERE
    pred.date BETWEEN DATE_SUB(DATE_SUB(DATE_TRUNC(DATE_SUB(DATE('{date_input}'), INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 7 DAY), INTERVAL 5 WEEK)
    AND DATE_SUB(DATE_SUB(DATE_TRUNC(DATE_SUB(DATE('{date_input}'), INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 1 DAY), INTERVAL 5 WEEK)
),
count_tp AS (
SELECT
    SUM (CASE WHEN label=1 AND p_churn=1 THEN 1 END) tp,
    SUM (CASE WHEN label=1 AND p_churn=0 THEN 1 END) fn,
    SUM (CASE WHEN label=0 AND p_churn=1 THEN 1 END) fp,
    SUM (CASE WHEN label=0 AND p_churn=0 THEN 1 END) tn
FROM
    prediction_data
)
SELECT
    *,
    SAFE_DIVIDE(2*precision*recall, precision+recall) f1,
    CURRENT_DATE('Asia/Jakarta') as created_at
FROM
    (SELECT
        DATE_SUB(DATE_SUB(DATE_TRUNC(DATE_SUB(DATE('{date_input}'), INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 7 DAY), INTERVAL 5 WEEK) week_date,
        SAFE_DIVIDE(tp,tp+fp) precision,
        SAFE_DIVIDE(tp,tp+fn) recall
    FROM
        count_tp)
