import pandas as pd
import logging
import warnings
import numpy as np
import pytz
from joblib import load
from datetime import datetime
from google.cloud import bigquery
from tqdm import tqdm
from module.bq_connection import BQConnection
from module.pipeline_config import churn_model
from module.base_func import BaseFunc

logging = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


class ChurnPrediction(BaseFunc):
    def __init__(
        self,
        credential_datamart,
        project_id_bq,
        churn_pred_config,
        batch_limit,
        date_variable
    ):
        super().__init__()
        self.credential_datamart = credential_datamart
        self.project_id = project_id_bq
        self.churn_pred_query_list = churn_pred_config["QUERY"]
        self.churn_pred_bq_tables_config = churn_pred_config["BQ_CONFIG"]
        self.view = self.churn_pred_bq_tables_config["CHURN_FEATURE_TEMP_TABLE"]
        self.dataset = self.churn_pred_bq_tables_config["CHURN_PRED_BQ_DB"]
        self.batch = batch_limit
        self.date_key = date_variable

        self.bq_final_table = self.churn_pred_bq_tables_config[
            "CHURN_SCORE_TABLE"
        ]
        self.bq_proportion_table = self.churn_pred_bq_tables_config[
            "CHURN_PRED_PROPORTION_TABLE"
        ]
        self.bq_psi_table = self.churn_pred_bq_tables_config["CHURN_PRED_PSI_TABLE"]

        self.churn_pred_bq_read_rows_query = self.churn_pred_query_list[
            "CHURN_PRED_BQ_READ_ROWS_QUERY"
        ].format(project_id = self.project_id, dataset = self.dataset, table_name = self.view)
        self.churn_pred_bq_temp_feature_query = self.churn_pred_query_list[
            "CHURN_PRED_BQ_TEMP_FEATURE_QUERY"
        ].format(project_id = self.project_id, dataset = self.dataset, table_name = self.churn_pred_bq_tables_config["CHURN_SOURCE_TABLE"],
                 date_input = date_variable
        )
        self.churn_pred_data_bin_train_path = self.churn_pred_bq_tables_config[
            "CHURN_PRED_TRAIN_BIN_DATA"
        ]
        self.bq_eval_table = self.churn_pred_bq_tables_config[
            "CHURN_PRED_EVAL_TABLE"
        ]
        self.churn_pred_bq_eval_query = self.churn_pred_query_list[
            "CHURN_PRED_BQ_EVAL_QUERY"
        ].format(project_id = self.project_id, dataset = self.dataset,
                 table_name = self.bq_eval_table ,
                 dm_pred_table = self.bq_final_table,
                 dm_source_table = self.churn_pred_bq_tables_config["CHURN_SOURCE_TABLE"],
                 date_input = date_variable)

        self.bq = BQConnection()

    def monitor_psi(self):
        try:
            psi_bin = pd.read_csv(self.churn_pred_data_bin_train_path)
            psi_bin.drop(columns=["Unnamed: 0"], inplace=True)
            col_list = [
                'SUM_PROMO_last90',
                'SUM_GMV_last90',
                'SUM_PROMO_last30days',
                'n_month_active',
                'SUM_PRDTRX_last30days',
                'SUM_PRDTRX_PROMO_last30days',
                'SUM_GMV_last30days',
                'SUM_GMV_growth',
                'PROMO_ratio_last30days',
                'SUM_PROMO_growth',
                'proba'
            ]

            converted_week = pd.to_datetime(self.date_key) - pd.Timedelta(days=1)
            converted_week = converted_week - pd.to_timedelta(converted_week.dayofweek, unit='d') - pd.Timedelta(days=7)
            converted_week = converted_week.strftime("%Y-%m-%d")

            df_psi_count = pd.DataFrame(columns=["feature", "bin", "count"])
            for i in col_list:
                if i == "n_month_active":
                    query = """
                    SELECT
                        COUNT(CASE WHEN n_month_active = 0 THEN 1 END) AS bin_0,
                        COUNT(CASE WHEN n_month_active = 1 THEN 1 END) AS bin_1,
                        COUNT(CASE WHEN n_month_active = 2 THEN 1 END) AS bin_2,
                        COUNT(CASE WHEN n_month_active = 3 THEN 1 END) AS bin_3,

                    FROM {project_id}.{dataset}.{table_name}
                    WHERE DATE(week_date) = '{week_date}'
                    """.format(
                        project_id = self.project_id,
                        dataset = self.dataset,
                        table_name = self.bq_final_table,
                        week_date = converted_week
                    )
                else:
                    query = """
                    SELECT """
                    for j in range(10):
                        ix = j + 1
                        if ix > 1 and ix < 10:
                            query += """COUNT(CASE WHEN {feature} > {min_value} AND {feature} <= {max_value}  THEN 1 END) AS bin_{ix},
                            """.format(
                                feature = psi_bin.loc[
                                    (psi_bin["feature"] == i) & (psi_bin["bin"] == ix),
                                    "feature",
                                ].values[0],
                                min_value = psi_bin.loc[
                                    (psi_bin["feature"] == i) & (psi_bin["bin"] == ix),
                                    "min",
                                ].values[0],
                                max_value =
                                psi_bin.loc[
                                    (psi_bin["feature"] == i) & (psi_bin["bin"] == ix),
                                    "eq_max",
                                ].values[0],
                                ix = ix,
                            )
                        elif ix == 10:
                            query += """COUNT(CASE WHEN {feature} > {min_value} THEN 1 END) AS bin_{ix}
                            FROM {project_id}.{dataset}.{table_name}
                                WHERE DATE(week_date) = '{week_date}'
                            """.format(
                                feature = psi_bin.loc[
                                    (psi_bin["feature"] == i) & (psi_bin["bin"] == ix),
                                    "feature",
                                ].values[0],
                                min_value = psi_bin.loc[
                                    (psi_bin["feature"] == i) & (psi_bin["bin"] == ix),
                                    "min",
                                ].values[0],
                                ix = ix,
                                project_id = self.project_id,
                                dataset = self.dataset,
                                table_name = self.bq_final_table,
                                week_date = converted_week
                            )
                        elif ix == 1:
                            query += """ COUNT(CASE WHEN {feature} <= {max_value} THEN 1 END) AS bin_{ix},
                            """.format(
                                feature = psi_bin.loc[
                                    (psi_bin["feature"] == i) & (psi_bin["bin"] == ix),
                                    "feature",
                                ].values[0],
                                max_value = psi_bin.loc[
                                    (psi_bin["feature"] == i) & (psi_bin["bin"] == ix),
                                    "eq_max",
                                ].values[0],
                                ix = ix,
                            )

                df_bin_count = self.bq.read_bq(
                    query, self.credential_datamart, self.project_id, None
                )
                df_bin_count = df_bin_count.T.reset_index()
                df_bin_count["index"] = df_bin_count["index"].map(lambda x: x[4:])
                df_bin_count.rename(columns={"index": "bin"}, inplace=True)
                df_bin_count["feature"] = i
                df_bin_count.rename(columns={0: "count"}, inplace=True)
                df_psi_count = df_psi_count._append(df_bin_count, ignore_index=True)
            if (
                df_psi_count[["feature", "count"]]
                .groupby(["feature"])
                .sum()
                .reset_index()["count"][0]
                != 0
            ):
                df_psi_count["proportion"] = df_psi_count["count"].div(
                    df_psi_count[["feature", "count"]]
                    .groupby(["feature"])
                    .sum()
                    .reset_index()["count"][0]
                )
            else:
                df_psi_count["proportion"] = 0

            df_psi_count[["bin", "proportion"]] = df_psi_count[
                ["bin", "proportion"]
            ].astype(float)

            logging.info(
                "Finished loading data to calculate proportion at {}".format(
                    datetime.today()
                )
            )

            # yesterday = date.today() + relativedelta(days=-7)
            # monday = yesterday - timedelta(days=yesterday.weekday())
            df_psi_count["week_date"] = pd.to_datetime(converted_week)

            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=[
                    bigquery.SchemaField("week_date", "DATE"),
                    bigquery.SchemaField("bin", "INTEGER"),
                    bigquery.SchemaField("proportion", "FLOAT"),
                ],
                # time_partitioning=bigquery.TimePartitioning(
                #     type_=bigquery.TimePartitioningType.DAY,
                #     field="week_date",  # field to use for partitioning
                # ),
            )
            jakarta_tz = pytz.timezone("Asia/Jakarta")
            jakarta_time = datetime.now(jakarta_tz)
            df_psi_count["created_at"] = jakarta_time
            self.bq.to_bq(
                df_psi_count.drop(columns=["count"]),
                self.dataset + "." + self.bq_proportion_table,
                self.credential_datamart,
                self.project_id,
                job_config,
            )
            logging.info(
                "Finished inserting rows of proportion data at {}".format(
                    datetime.today()
                )
            )

            psi_bin = psi_bin.merge(df_psi_count, on=["feature", "bin"])
            psi_bin["current_min_train"] = (
                psi_bin["proportion"] - psi_bin["percent_to_total"]
            )
            psi_bin["ln_current_to_train"] = np.log(
                psi_bin["proportion"] / psi_bin["percent_to_total"]
            )
            psi_bin["psi"] = (
                psi_bin["ln_current_to_train"] * psi_bin["current_min_train"]
            )
            psi_final = (
                psi_bin[["feature", "psi"]]
                .replace(np.inf, 0)
                .groupby("feature")
                .sum()
                .reset_index()
            )
            psi_final["week_date"] = pd.to_datetime(converted_week)
            jakarta_tz = pytz.timezone("Asia/Jakarta")
            jakarta_time = datetime.now(jakarta_tz)
            psi_final["created_at"] = jakarta_time

            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=[
                    bigquery.SchemaField("week_date", "DATE"),
                    bigquery.SchemaField("psi", "FLOAT"),
                    bigquery.SchemaField("created_at", "DATETIME"),
                ],
                # time_partitioning=bigquery.TimePartitioning(
                #     type_=bigquery.TimePartitioningType.DAY,
                #     field="week_date",  # field to use for partitioning
                # ),
            )
            self.bq.to_bq(
                psi_final,
                self.dataset + "." + self.bq_psi_table,
                self.credential_datamart,
                self.project_id,
                job_config,
            )
            message = "Finished inserting rows of PSI data at {}".format(
                datetime.today()
            )
            logging.info(message)
            return message, 0
        except Exception as err:
            logging.error(err)
            return err, -1

    def eval_process(self):
        try:
            self.bq.dml_bq(
                self.churn_pred_bq_eval_query, self.credential_datamart, self.project_id
            )
            message = "Finished inserting rows of evaluation table {}".format(
                datetime.today()
            )
            logging.info(message)
            return message, 0
        except Exception as err:
            logging.error(err)
            return err, -1

    def predict_process(self, df):
        try:
            model_churn = load(churn_model)
            logging.info("finished loading churn pred model")
            col_pred = [
                'SUM_PROMO_last90',
                'SUM_GMV_last90',
                'SUM_PROMO_last30days',
                'n_month_active',
                'SUM_PRDTRX_last30days',
                'SUM_PRDTRX_PROMO_last30days',
                'SUM_GMV_last30days',
                'SUM_GMV_growth',
                'PROMO_ratio_last30days',
                'SUM_PROMO_growth'
            ]

            df["proba"] = model_churn.predict_proba(df[col_pred])[:, 1]
            message = "Successfuly, run predict_process, next data will be stored on batch operation"
            status = 0
            return df, message, status
        except Exception as err:
            logging.error(err)
            return None, err, -1

    def predict(self, begin, end, batch):
        try:
            logging.debug(
                "Connecting with BigQuery API for churn prediction batch ...".format(
                    batch
                )
            )
            logging.debug(
                "churn pred feature data loading batch {} starts at {}".format(
                    batch, datetime.today()
                )
            )
            client = bigquery.Client(
                credentials=self.credential_datamart, project=self.project_id
            )
            query = """
                    SELECT  *
                    FROM
                    `{}.{}`
                    WHERE
                    row_number between {} and {}
                    """.format(
                self.dataset, self.view, begin, end
            )
            query_job = client.query(query)
            df = query_job.result()
            df = df.to_dataframe()
            df = df.drop(columns="row_number")
            logging.info(
                "Success: load churn pred batch {} from BQ at {}".format(
                    batch, datetime.today()
                )
            )
            logging.debug(
                "Churn Prediction batch {} begin at {}".format(batch, datetime.today())
            )
            df, message, status = self.predict_process(df)
            if status == -1:
                return message, status
            logging.info(
                "Churn Prediction batch {} finish at {}".format(batch, datetime.today())
            )
            jakarta_tz = pytz.timezone("Asia/Jakarta")
            jakarta_time = datetime.now(jakarta_tz)
            df["created_at"] = jakarta_time
            df["week_date"] = pd.to_datetime(df["week_date"])
            df["date"] = pd.to_datetime(df["date"])

            write_config = "WRITE_APPEND"
            job_config = bigquery.LoadJobConfig(
                write_disposition=write_config,
                schema=[
                    bigquery.SchemaField("date", "DATETIME"),
                    bigquery.SchemaField("week_date", "DATE"),
                    bigquery.SchemaField("created_at", "DATETIME"),
                ],
                # time_partitioning=bigquery.TimePartitioning(
                #     type_=bigquery.TimePartitioningType.DAY,
                #     field="week_date",  # field to use for partitioning
                # ),
            )

            df["created_at"] = jakarta_time
            self.bq.to_bq(
                df,
                self.dataset + "." + self.bq_final_table,
                self.credential_datamart,
                self.project_id,
                job_config,
            )
            message = "Finished inserting rows from {} to {} at {}".format(
                begin, end, datetime.today()
            )
            logging.info(message)
            return message, 0
        except Exception as err:
            logging.error(err)
            return err, -1

    def predict_churn(self):
        try:
            logging.info("Churn prediction: Ready to process")
            message, status = self.create_feature_table(
                self.credential_datamart,
                self.project_id,
                self.dataset,
                self.view,
                self.churn_pred_bq_temp_feature_query,
            )
            if status == -1:
                output = {"message": message, "status": status}
                return output

            result_row, message, status = self.load_total_row(
                self.churn_pred_bq_read_rows_query,
                self.credential_datamart,
                self.project_id,
                "churn_prediction_feature",
            )
            if status == -1:
                output = {"message": message, "status": status}
                return output

            batch_limit = int(self.batch)
            if (batch_limit == 0) or (result_row < batch_limit):
                batch_limit = result_row
            batch = 0
            status_batch = 0
            for i in tqdm(range(0, result_row, batch_limit)):
                begin = i + 1
                end = i + batch_limit
                if end > result_row:
                    end = result_row
                batch += 1
                message, status_temp = self.predict(begin, end, batch)
                if status_temp == -1:
                    status_batch = -1
                    message_batch = message

            message, status = self.monitor_psi()
            if status == -1:
                output = {"message": message, "status": status}
                return output

            message, status = self.eval_process()
            if status == -1:
                output = {"message": message, "status": status}
                return output

            if status == 0 and status_batch == -1:
                message = "processes finished, yet churn prediction was failed on some batch. {}".format(
                    str(message_batch)
                )
                status = status_batch
            output = {"message": message, "status": status}
        except Exception as err:
            logging.error("Error: {}".format(err))
            output = {"message": err, "status": -1}
        return output
