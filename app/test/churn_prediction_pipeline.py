import os
import unittest
import uuid
from module.churn_prediction_pipeline import ChurnPredictProcess
from module.pipeline_config import GENERAL_CONFIG, CHURN_PRED_CONFIG
from module import task_manager

class ChurnPredGenerationPipeline(unittest.TestCase):
    @unittest.skip("Churn Prediction Generation Pipeline skipped")
    def test_churn_generation(self):
        print("Start Testing Churn Prediction Generation")
        tm_conn = task_manager.create_connection(
            os.getcwd() + "/sqlitedb/tasks_test.sqlite3"
        )
        task_manager.create_table(tm_conn)
        generating_table = "churn_prediction"
        batch_limit = int(self.getArgument("batch_limit", self.batch_limit))
        date_variable = int(self.getArgument("date_variable", self.date_variable))
        task_id = str(uuid.uuid4())
        task_name = "Generate_table_" + str(generating_table)
        credential_datamart = GENERAL_CONFIG["CREDENTIAL"]
        project_id_bq = GENERAL_CONFIG["PROJECTID"]

        churn_pred_process = ChurnPredictProcess(
            tm_conn,
            batch_limit,
            task_id,
            task_name,
            generating_table,
            credential_datamart,
            project_id_bq,
            CHURN_PRED_CONFIG,
            date_variable
        )
        churn_pred_process.generate_churn_pred()


if __name__ == "__main__":
    unittest.main()
