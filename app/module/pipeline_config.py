import os
import json
import traceback
import logging
from pathlib import Path
from dotenv import load_dotenv
from google.oauth2 import service_account
from google.cloud import bigquery

logger = logging.getLogger(__name__)

load_dotenv()

# credential
credential_datamart = Path(__file__).parent.parent.resolve() / str(os.getenv("BQ_CREDENTIAL"))
project_id_bq = os.getenv("BQ_DATAMART_PROJECT_ID")

# query_path
query_read_rows_bq_path = Path(__file__).parent.parent.resolve() / str(os.getenv("READ_ROWS_QUERY"))
query_churn_pred_feature_bq_path = Path(__file__).parent.parent.resolve() / str(os.getenv("CHURN_PRED_TEMP_FEATURE_QUERY"))
query_churn_pred_eval_bq_path = Path(__file__).parent.parent.resolve() / str(os.getenv("CHURN_PRED_EVAL_QUERY"))
query_search_customer_id_path = Path(__file__).parent.parent.resolve() / str(os.getenv("CHECK_CUSTOMER_ID_QUERY"))
query_search_churn_pred_path = Path(__file__).parent.parent.resolve() / str(os.getenv("CHECK_CHURN_PRED_QUERY"))

try:
    if os.getenv("IS_SERVICE_ACCOUNT_FROM_FILE").lower() == "true":
        logger.info("Reading BigQuery credentials from files - churn pipeline")
        credential_datamart = service_account.Credentials.from_service_account_file(
            credential_datamart
        )
    else:
        credential_datamart = service_account.Credentials.from_service_account_info(
            json.loads(credential_datamart)
        )
        logger.info("Credentials are not from files")

    bq_read_rows_query = open(query_read_rows_bq_path).read()
    churn_pred_bq_temp_feature_query = open(query_churn_pred_feature_bq_path).read()
    churn_pred_bq_eval_query = open(query_churn_pred_eval_bq_path).read()
except Exception as e:
    logger.error(
        "query or credentials for retrieving dataset for churn pipeline is not available. error {e}. traceback {tr}".format(
        e=e,
        tr=traceback.format_exc()
        )
    )
    raise ValueError(
        "query or credentials for churn pipeline is not available, {}".format(e)
    )

batch_limit = os.getenv("PIPELINE_GENERATE_BATCH_LIMIT")

GENERAL_CONFIG = {
    "BATCH": batch_limit,
    "CREDENTIAL": credential_datamart,
    "PROJECTID": project_id_bq,
}

# churn pred model path
churn_model = str(os.getenv("CHURN_PRED_MODEL"))

# churn pred query list
CHURN_PRED_CONFIG = {
    "QUERY": {
        "CHURN_PRED_BQ_TEMP_FEATURE_QUERY": churn_pred_bq_temp_feature_query,
        "CHURN_PRED_BQ_READ_ROWS_QUERY": bq_read_rows_query,
        "CHURN_PRED_BQ_EVAL_QUERY": churn_pred_bq_eval_query,
    },
    "BQ_CONFIG": {
        "CHURN_PRED_BQ_DB": os.getenv("CHURN_BQ_DB"),
        "CHURN_SOURCE_TABLE" : os.getenv("SOURCE_TABLE"),
        "CHURN_FEATURE_TEMP_TABLE": os.getenv("CHURN_FEATURE_TEMP_TABLE"),
        "CHURN_SCORE_TABLE": os.getenv("CHURN_SCORE_TABLE"),
        "CHURN_PRED_MAX_WORKERS": os.getenv("CHURN_PRED_MAX_WORKERS"),
        "CHURN_PRED_PSI_TABLE": os.getenv("CHURN_PRED_PSI_TABLE"),
        "CHURN_PRED_PROPORTION_TABLE": os.getenv("CHURN_PRED_PROPORTION_TABLE"),
        "CHURN_PRED_TRAIN_BIN_DATA": str(os.getenv("CHURN_PRED_TRAIN_BIN_DATA")),
        "CHURN_PRED_EVAL_TABLE": os.getenv("CHURN_PRED_EVAL_TABLE"),
    },
}

# getting churn score
search_customer_id_query = open(query_search_customer_id_path).read()
search_churn_pred_query = open(query_search_churn_pred_path).read()

# getting churn score by customer_id list
SEARCH_CONFIG = {
    "QUERY": {
        "SEARCH_CUSTOMER_ID_QUERY": search_customer_id_query,
        "SEARCH_CHURN_PRED_QUERY": search_churn_pred_query,
    },
    "BQ_CONFIG": {
        "SEARCH_BQ_DB": os.getenv("CUSTOMER_ID_BQ_DB"),
        "SEARCH_BQ_TABLE": os.getenv("CUSTOMER_ID_BQ_TABLE"),
        "CHURN_PRED_SQL_TABLE" : os.getenv("CHURN_SCORE_TABLE"),
        "CHURN_SQL_LOG" : os.getenv("CHURN_SQL_LOG")
    },
}

# job config
job_config_batch = bigquery.QueryJobConfig(
    # Run at batch priority, which won't count toward concurrent rate limit.
    priority=bigquery.QueryPriority.BATCH
)

job_config_large_rows = bigquery.QueryJobConfig(
    # Run at batch priority, which won't count toward concurrent rate limit.
    allow_large_results=True,
    priority=bigquery.QueryPriority.BATCH,
)
