import logging
import warnings
from concurrent.futures import ProcessPoolExecutor
from module.task_manager import update_task
from datetime import datetime
from module.churn_prediction import ChurnPrediction



logging = logging.getLogger(__name__)

warnings.filterwarnings("ignore")


class ChurnPredictProcess(object):
    def __init__(
        self,
        sqliteConn,
        batch_limit,
        task_id,
        task_name,
        generating_table,
        credential_datamart,
        project_id_bq,
        churn_pred_config,
        date_variable
    ):
        self.churn_pred_config = churn_pred_config
        self.churn_pred_query_list = churn_pred_config["QUERY"]
        self.churn_pred_bq_tables_config = churn_pred_config["BQ_CONFIG"]
        self.date_variable = date_variable
        self.sqliteConn = sqliteConn
        self.batch_limit = batch_limit
        self.task_id = task_id
        self.task_name = task_name
        self.task_start_date = datetime.today()
        self.type = generating_table
        self.credential_datamart = credential_datamart
        self.project_id_bq = project_id_bq

    def generate_churn_pred(self):
        def predict_callback(future):
            message = "Success"
            try:
                output = future.result()
                logging.info("output: {}".format(output))
                if output["status"] != -1:
                    logging.info(
                        "Output succesfully retrieved with total row = {}".format(
                            output["message"]
                        )
                    )
                    task_status = 0  # done
                    task_end_date = datetime.today()
                    message += " all data {} have been finished to save in db".format(
                        self.type
                    )
                else:
                    logging.error("future training error: {}".format(output["message"]))
                    task_status = -1  # status error
                    task_end_date = datetime.today()
                    message = "Error: " + str(output["message"])

            except Exception as ex:
                logging.error("future training error: {}".format(ex))
                task_status = -1  # status error
                task_end_date = datetime.today()
                message = "Error " + str(ex)

            update_task(
                self.sqliteConn,
                self.task_id,
                self.task_name,
                task_status,
                self.task_start_date,
                task_end_date,
                message,
                types="update",
            )
            logging.info("Task Status Completed: {}".format(message))

        task_status = 1
        logging.info("task id: {}".format(self.task_id))

        update_task(
            self.sqliteConn,
            self.task_id,
            self.task_name,
            task_status,
            self.task_start_date,
            "null",
            types="insert",
        )
        executor = ProcessPoolExecutor(
            max_workers=int(
                self.churn_pred_bq_tables_config["CHURN_PRED_MAX_WORKERS"]
            )
        )
        churn_pred = ChurnPrediction(
            self.credential_datamart,
            self.project_id_bq,
            self.churn_pred_config,
            self.batch_limit,
            self.date_variable
        )

        worker = executor.submit(churn_pred.predict_churn)
        worker.add_done_callback(predict_callback)

        reason = "Table prediction request accepted and running in the background. Check on /job_info to know all the tasks."
        additional_response = {"data": str(self.task_id)}

        return reason, additional_response
