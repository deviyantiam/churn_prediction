import logging
import warnings
from datetime import datetime
from module.bq_connection import BQConnection


logging = logging.getLogger(__name__)
warnings.filterwarnings("ignore")


class BaseFunc:
    def __init__(self):
        self.bq = BQConnection()

    def load_total_row(self, query, credential, project_id, process_name):
        try:
            logging.debug("Reading BigQuery {} temp table ...".format(process_name))
            logging.info(
                "Start to count total row of {} temp table from BQ {}".format(
                    process_name, datetime.today()
                )
            )
            results = self.bq.read_bq(query, credential, project_id, None)
            rows_total = results.values[0][0]

            message = "Success, temp {} table has {} rows at {}".format(
                process_name, rows_total, datetime.today()
            )
            logging.info(message)
            return rows_total, message, 0
        except Exception as err:
            logging.error(err)
            return 0, err, -1

    def create_feature_table(self, credential, project_id, dataset, table_name, query):
        try:
            logging.debug(
                "Create temporary table {}.{}.{} at progress to table at {}".format(
                    project_id, dataset, table_name, datetime.today()
                )
            )
            self.bq.create_table_feature(
                query, credential, project_id, dataset, table_name
            )
            logging.debug(
                "Finished temporary {}.{}.{} table at {}".format(
                    project_id, dataset, table_name, datetime.today()
                )
            )
            message = "Success create view {}.{}.{}".format(
                project_id, dataset, table_name
            )
            logging.info(message)
            return message, 0
        except Exception as err:
            logging.error(err)
            return err, -1
