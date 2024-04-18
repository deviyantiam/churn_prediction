import pandas as pd
import logging
import warnings
import pytz
from google.cloud import bigquery
from datetime import datetime, timedelta
from module.bq_connection import BQConnection

logging = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


class SearchChurnScore:
    def __init__(self, credential_datamart, project_id_bq, search_config):
        self.credential_datamart = credential_datamart
        self.project_id = project_id_bq
        self.query_list = search_config["QUERY"]
        self.bq_tables_config = search_config["BQ_CONFIG"]
        self.dataset = self.bq_tables_config["SEARCH_BQ_DB"]
        self.customer_id_table = self.bq_tables_config["SEARCH_BQ_TABLE"]
        self.churn_pred_table = self.bq_tables_config["CHURN_PRED_SQL_TABLE"]

        self.bq = BQConnection()

        self.query_search_customer_id = self.query_list["SEARCH_CUSTOMER_ID_QUERY"]
        self.query_search_churn_pred = self.query_list["SEARCH_CHURN_PRED_QUERY"]
        self.churn_log_table = self.bq_tables_config["CHURN_SQL_LOG"]

    def check_customer_id(self, customer_id):
        query = self.query_search_customer_id.format(master_customer_table =
         self.project_id+"."+self.dataset+"."+self.customer_id_table, customer_id = customer_id)
        data = self.bq.read_bq(
                    query, self.credential_datamart, self.project_id, None
                )
        return data


    def get_churn_pred(self, customer_id):
        query = self.query_search_churn_pred.format(
              full_table_name = self.project_id+"."+self.dataset+"."+self.churn_pred_table, customer_id = customer_id)
        data_churn_pred = self.bq.read_bq(
                    query, self.credential_datamart, self.project_id, None
                )
        data_churn_pred = data_churn_pred[["customer_id", "p_churn"]]
        reason = "Success, retrieving churn pred"
        if len(data_churn_pred) < 1:
            data_churn_pred = pd.DataFrame(columns=["customer_id", "p_churn"])
            data_churn_pred.loc[0, "customer_id"] = customer_id
        return data_churn_pred, reason

    def get_churn_prediction(self, customer_id):
        data_customer_id = self.check_customer_id(customer_id)
        if data_customer_id["count_row"][0] > 0:
            df_churn_pred, reason = self.get_churn_pred(customer_id)
        else:
            df_churn_pred = pd.DataFrame(
                columns=["customer_id", "p_churn"]
            )
            df_churn_pred.loc[0, "customer_id"] = customer_id
            reason = "Success, no customer_id found"
        return df_churn_pred, reason

    def insert_response(self, customer_id, task_id, data, status):
        save_err_messages = None
        try:
            jakarta_tz = pytz.timezone("Asia/Jakarta")
            jakarta_time = datetime.now(jakarta_tz)
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=[
                    bigquery.SchemaField("created_at", "TIMESTAMP"),
                ],
                # time_partitioning=bigquery.TimePartitioning(
                #     type_=bigquery.TimePartitioningType.DAY,
                #     field="created_at",  # field to use for partitioning
                # ),
            )
            df_log = {"task_id": [task_id], "customer_id": [customer_id], "response" : [data], "status": status, "created_at": jakarta_time}
            df_log = pd.DataFrame(df_log)
            self.bq.to_bq(
                df_log,
                self.dataset + "." + self.churn_log_table,
                self.credential_datamart,
                self.project_id,
                job_config,
            )

            logging.info(
                "Record {} with data:{} inserted successfully".format(
                    self.churn_log_table, data
                )
            )
            message = "Finished inserting rows of evaluation table {}".format(
                datetime.today()
            )
            logging.info(message)
            return message, 0

        except Exception as error:
            logging.error("Error while inserting log response {}".format(error))
            save_err_messages = str(error)

        return save_err_messages
