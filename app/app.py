import os
import urllib
import uuid
import json
import traceback
import tornado
import logging
import asyncio
import module.task_manager as task_manager
from dotenv import load_dotenv
from config import APP_CONFIG
from tornado.web import Application, RequestHandler
from module.utils import create_response, Hello, CheckTask
from module.pipeline_config import (
    GENERAL_CONFIG,
    CHURN_PRED_CONFIG,
    SEARCH_CONFIG,
)
from module.getting_churn_score import SearchChurnScore
from module.churn_prediction_pipeline import ChurnPredictProcess


load_dotenv()

service_logger = logging.getLogger(__name__)


class GenerateChurnPred(RequestHandler):
    SUPPORTED_METHODS = ["POST"]

    def prepare(self):
        super(GenerateChurnPred, self).prepare()
        self.json_data = None
        if self.request.body:
            try:
                self.json_data = tornado.escape.json_decode(self.request.body)
            except ValueError as e:
                service_logger.error("Cannot decode request. error = {}".format(e))

    def initialize(self, sqliteConn, general_config, churn_pred_config):
        self.sqliteConn = sqliteConn
        self.credential_datamart = general_config["CREDENTIAL"]
        self.project_id_bq = general_config["PROJECTID"]
        self.churn_pred_config = churn_pred_config
        self.batch_limit = general_config["BATCH"]

    def getArgument(self, arg, default=None):
        if self.request.method in ["POST"] and self.json_data:
            return self.json_data.get(arg, default)
        else:
            return super(GenerateChurnPred, self).getArgument(arg, default)

    def post(self):
        self.set_header("Content-Type", "application/json")
        try:
            request_params = json.loads(self.request.body)
        except:
            request_params = json.loads(urllib.parse.unquote_plus(self.request.body))

        service_logger.info("req param : {}".format(request_params))
        generating_table = "churn_prediction"
        batch_limit = int(self.getArgument("batch_limit", self.batch_limit))

        date_variable = self.getArgument("date_variable", None)
        task_id = str(uuid.uuid4())
        task_name = "Generate_table_" + str(generating_table)


        check = CheckTask(self.sqliteConn, task_name)
        stop = check.check_running_task(self)
        if stop == 1:
            return

        churn_pred_process = ChurnPredictProcess(
            self.sqliteConn,
            batch_limit,
            task_id,
            task_name,
            generating_table,
            self.credential_datamart,
            self.project_id_bq,
            self.churn_pred_config,
            date_variable
        )
        try:
            reason, add_reason = churn_pred_process.generate_churn_pred()
            status = 200
            create_response(
                self, response_code=status, reason=reason, additional_response=add_reason
            )
        except Exception as e:
            reason = "Error submitting the process."
            additional_response = {"error": str(e)}
            service_logger.error(reason + ' error {}, traceback {}'.format(e, traceback.format_exc()))
            create_response(
                self,
                response_code=500,
                response_status="UNKNOWN",
                reason=reason,
                additional_response=additional_response,
            )
        return

class JobInfo(RequestHandler):
    def initialize(self, sqliteConn):
        self.sqliteConn = sqliteConn

    def get(self):
        try:
            request_params = json.loads(self.request.body)
        except:
            request_params = json.loads(urllib.parse.unquote_plus(self.request.body))
        key = request_params.keys()
        status = 200
        reason = "All task running"
        if len(key) > 0:
            id = request_params["id"]
            if id is not None or id != "":
                service_logger.info("id task:{}".format(id))
                tasklist = task_manager.select_task(self.sqliteConn, id)
                service_logger.info("task list: {}".format(tasklist))
                if len(tasklist) != 0:
                    add_reason = {
                        "task_id": tasklist[0][0],
                        "task_name": tasklist[0][1],
                        "status": tasklist[0][2],
                        "begin_time": tasklist[0][3],
                        "end_time": tasklist[0][4],
                        "messages": tasklist[0][5],
                    }
                else:
                    tasklist = task_manager.select_all_tasks(self.sqliteConn)
                    add_reason = {"data": tasklist}
        else:
            tasklist = task_manager.select_all_tasks(self.sqliteConn)
            add_reason = {"data": tasklist}
        create_response(
            self,
            response_code=status,
            reason=reason,
            additional_response=add_reason,
            encode_numpy=True,
        )
        return

class GetChurnScore(RequestHandler):
    SUPPORTED_METHODS = ["POST"]

    def prepare(self):
        super(GetChurnScore, self).prepare()
        self.json_data = None
        if self.request.body:
            try:
                self.json_data = tornado.escape.json_decode(self.request.body)
            except ValueError as e:
                service_logger.error("Cannot decode request. error {}".format(e))

    def initialize(self, sqliteConn, general_config, search_config):
        self.sqliteConn = sqliteConn
        self.credential_datamart = general_config["CREDENTIAL"]
        self.project_id_bq = general_config["PROJECTID"]
        self.search_config = search_config
        self.query_list = self.search_config["QUERY"]
        self.bq_config = self.search_config["BQ_CONFIG"]

    def getArgument(self, arg, default=None):
        if self.request.method in ["POST"] and self.json_data:
            return self.json_data.get(arg, default)
        else:
            return super(GetChurnScore, self).getArgument(arg, default)

    def post(self):
        self.set_header("Content-Type", "application/json")
        try:
            request_params = json.loads(self.request.body)
        except:
            request_params = json.loads(urllib.parse.unquote_plus(self.request.body))

        service_logger.info("req param : {}".format(request_params))
        customer_id = self.getArgument("customer_id", None)
        task_id = str(uuid.uuid4())
        try:
            if len(str(customer_id).lower()) > 0:
                search = SearchChurnScore(self.credential_datamart, self.project_id_bq, self.search_config)
                data_churn, reason = search.get_churn_prediction(customer_id)
                add_reason = {"data": json.loads(data_churn.to_json(orient="records"))}

            else:
                reason = "Please input customer_id to get churn score"
                status = 400
                add_reason = {}
                create_response(
                    self,
                    response_code=status,
                    reason=reason,
                    additional_response=add_reason,
                )
                return

            status = 200
            create_response(
                self,
                response_code=status,
                reason=reason,
                additional_response=add_reason,
            )
            data = json.dumps(
                json.loads(data_churn.to_json(orient="records", lines=True))
            )
            search.insert_response(customer_id, task_id, data, status)
            return

        except Exception as e:
            reason = "Error while searching"
            service_logger.error(reason + ' error {}, traceback {}'.format(e, traceback.format_exc()))
            additional_response = {"error": str(e)}
            create_response(
                self,
                response_code=500,
                reason=reason,
                additional_response=additional_response,
            )
            return


def make_app(
    sqliteConn=None,
    general_config=None,
    churn_pred_config=None,
    search_config=None,
):
    settings = {"debug": True}
    return Application(
        [   (r"/", Hello),
            (
                r"/churn_score_generation",
                GenerateChurnPred,
                dict(
                    sqliteConn=sqliteConn,
                    general_config=general_config,
                    churn_pred_config=churn_pred_config
                ),
            ),
            (r"/job_info", JobInfo, dict(sqliteConn=sqliteConn)),
            (
                r"/churn_prediction",
                GetChurnScore,
                dict(
                    sqliteConn=sqliteConn,
                    general_config=general_config,
                    search_config=search_config
                ),
            ),
        ],
        **settings
    )


def main(
    sqliteConn=None,
    general_config=None,
    churn_pred_config=None,
    search_config=None,
    PORT=None,
    HOST="localhost",
):
    service_logger.info("Starting Churn batch prediction ...")
    service_logger.debug("Current working directory: {}".format(os.getcwd()))
    service_logger.info("Host: {}, Port: {} ".format(HOST, PORT))
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    app = make_app(
        sqliteConn=sqliteConn,
        general_config=general_config,
        churn_pred_config=churn_pred_config,
        search_config=search_config,
    )
    app.listen(PORT, address=HOST)
    print("*** Application is ready! ***")
    service_logger.info("Application is ready to use.")
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    load_dotenv()
    task_manager.validate_db_file(os.getcwd() + "/sqlitedb/tasks.sqlite3")
    tm_conn = task_manager.create_connection(os.getcwd() + "/sqlitedb/tasks.sqlite3")
    task_manager.create_table(tm_conn)
    port = APP_CONFIG["PORT"]
    host = APP_CONFIG["HOST"]
    main(
        sqliteConn=tm_conn,
        general_config=GENERAL_CONFIG,
        churn_pred_config=CHURN_PRED_CONFIG,
        search_config=SEARCH_CONFIG,
        PORT=port,
        HOST=host,
    )
