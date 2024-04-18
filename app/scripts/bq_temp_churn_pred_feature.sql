WITH
amount AS (
SELECT
  DISTINCT
  date,
  DATE_TRUNC(date, WEEK(MONDAY)) week_date,
  customer_id,
  SUM(CASE WHEN quantity>0 then total_sales end) OVER (PARTITION BY customer_id ORDER BY UNIX_DATE(date) RANGE BETWEEN 30 PRECEDING AND 1 PRECEDING) AS SUM_GMV_last30days,
  SUM(CASE WHEN quantity>0 then total_sales end) OVER (PARTITION BY customer_id ORDER BY UNIX_DATE(date) RANGE BETWEEN 60 PRECEDING AND 31 PRECEDING) AS SUM_GMV_last30_60days,
  SUM(CASE WHEN quantity>0 then total_sales end) OVER (PARTITION BY customer_id ORDER BY UNIX_DATE(date) RANGE BETWEEN 90 PRECEDING AND 61 PRECEDING) AS SUM_GMV_last60_90days,
  SUM(CASE WHEN quantity>0 then discount end) OVER (PARTITION BY customer_id ORDER BY UNIX_DATE(date) RANGE BETWEEN 30 PRECEDING AND 1 PRECEDING) AS SUM_PROMO_last30days,
  SUM(CASE WHEN quantity>0 then discount end) OVER (PARTITION BY customer_id ORDER BY UNIX_DATE(date) RANGE BETWEEN 60 PRECEDING AND 31 PRECEDING) AS SUM_PROMO_last30_60days,
  SUM(CASE WHEN quantity>0 then discount end) OVER (PARTITION BY customer_id ORDER BY UNIX_DATE(date) RANGE BETWEEN 90 PRECEDING AND 61 PRECEDING) AS SUM_PROMO_last60_90days,
  SUM(CASE WHEN quantity>0 and discount>0 then 1 end) OVER (PARTITION BY customer_id ORDER BY UNIX_DATE(date) RANGE BETWEEN 30 PRECEDING AND 1 PRECEDING) AS SUM_PRDTRX_PROMO_last30days,
  COUNT(CASE WHEN quantity>0 then invoice_id end) OVER (PARTITION BY customer_id ORDER BY UNIX_DATE(date) RANGE BETWEEN 30 PRECEDING AND 1 PRECEDING) AS SUM_PRDTRX_last30days
FROM
  {project_id}.{dataset}.{table_name}
WHERE
  date BETWEEN DATE_SUB(DATE_SUB(DATE_TRUNC(DATE_SUB(date('{date_input}'),INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 7 DAY), INTERVAL 90 DAY) AND DATE_SUB(DATE_TRUNC(DATE_SUB(date('{date_input}'),INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 1 DAY)
),
trx AS (
SELECT
  tb1.date,
  DATE_TRUNC(tb1.date, WEEK(MONDAY)) week_date,
  tb1.customer_id,
  COUNT(DISTINCT CASE WHEN tb2.quantity>0 AND DATE_DIFF(tb1.date,tb2.date,DAY)<=30 THEN tb2.invoice_id END) AS SUM_TRX_last30days,
  COUNT(DISTINCT CASE WHEN tb2.quantity>0 AND DATE_DIFF(tb1.date,tb2.date,DAY) BETWEEN 31 AND 60 THEN tb2.invoice_id END) AS SUM_TRX_last30_60days,
  COUNT(DISTINCT CASE WHEN tb2.quantity>0 AND DATE_DIFF(tb1.date,tb2.date,DAY) BETWEEN 61 AND 90 THEN tb2.invoice_id END) AS SUM_TRX_last60_90days
FROM
(SELECT * FROM
  {project_id}.{dataset}.{table_name}
WHERE
  date BETWEEN DATE_SUB(DATE_SUB(DATE_TRUNC(DATE_SUB(date('{date_input}'),INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 7 DAY), INTERVAL 90 DAY) AND DATE_SUB(DATE_TRUNC(DATE_SUB(date('{date_input}'),INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 1 DAY)
) tb1
LEFT JOIN (SELECT * FROM
  {project_id}.{dataset}.{table_name}
WHERE
  date BETWEEN DATE_SUB(DATE_SUB(DATE_TRUNC(DATE_SUB(date('{date_input}'),INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 7 DAY), INTERVAL 90 DAY) AND DATE_SUB(DATE_TRUNC(DATE_SUB(date('{date_input}'),INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 1 DAY)
) tb2 on tb2.date<tb1.date and tb2.customer_id=tb1.customer_id
GROUP BY 1,2,3
),
combined AS (
SELECT
  COALESCE(trx.customer_id,amount.customer_id) customer_id,
  DATE(COALESCE(trx.date,amount.date)) date,
  COALESCE(trx.week_date,amount.week_date) week_date,
  COALESCE(amount.SUM_PROMO_last30days,0)+COALESCE(amount.SUM_PROMO_last30_60days,0)+COALESCE(amount.SUM_PROMO_last60_90days,0) SUM_PROMO_last90,
  COALESCE(amount.SUM_GMV_last30days,0)+COALESCE(amount.SUM_GMV_last30_60days,0)+COALESCE(amount.SUM_GMV_last60_90days,0) SUM_GMV_last90,
  COALESCE(amount.SUM_PROMO_last30days,0) SUM_PROMO_last30days,
  CASE WHEN SUM_TRX_last60_90days>1 THEN 3
    WHEN SUM_TRX_last30_60days>1 THEN 2
    WHEN SUM_TRX_last30days>1 THEN 1
    ELSE 0 END n_month_active,
  COALESCE(amount.SUM_PRDTRX_last30days,0)  SUM_PRDTRX_last30days,
  COALESCE(amount.SUM_PRDTRX_PROMO_last30days,0)  SUM_PRDTRX_PROMO_last30days,
  COALESCE(amount.SUM_GMV_last30days,0) SUM_GMV_last30days,
  COALESCE(SAFE_DIVIDE(amount.SUM_GMV_last30days,amount.SUM_GMV_last30_60days),0) SUM_GMV_growth,
  COALESCE(SAFE_DIVIDE(amount.SUM_PROMO_last30days,amount.SUM_GMV_last30days),0) PROMO_ratio_last30days,
  COALESCE(SAFE_DIVIDE(amount.SUM_PROMO_last30days,amount.SUM_PROMO_last30_60days),0) SUM_PROMO_growth,
FROM
  amount
FULL OUTER JOIN trx ON trx.date=amount.date AND trx.customer_id=amount.customer_id
),
quali AS (
SELECT *
FROM
    combined
QUALIFY ROW_NUMBER()OVER(PARTITION BY customer_id, week_date ORDER BY date DESC)=1
)
SELECT
  *,
  ROW_NUMBER () OVER () as row_number
FROM
  combined
WHERE date BETWEEN DATE_SUB(DATE_TRUNC(DATE_SUB(date('{date_input}'),INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 7 DAY) AND DATE_SUB(DATE_TRUNC(DATE_SUB(date('{date_input}'),INTERVAL 1 DAY), WEEK(MONDAY)), INTERVAL 1 DAY)
