import os
from dotenv import load_dotenv

load_dotenv()

# credential
credential_datamart = str(os.getcwd()) + str(os.getenv("BQ_CREDENTIAL"))
project_id_bq = os.getenv("BQ_DATAMART_PROJECT_ID")

churn_bq_db = os.getenv("CHURN_BQ_DB")
churn_feature_table = os.getenv("CHURN_FEATURE_TABLE")
churn_result_table = os.getenv("CHURN_RESULT_TABLE")
churn_score_result_table = os.getenv("CHURN_SCORE_TABLE")