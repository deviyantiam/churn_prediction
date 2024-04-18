import os
import json
import numpy as np
from tornado.web import RequestHandler
from module.task_manager import select_all_tasks


class Service(object):
    def __init__(self, port, host="0.0.0.0", server=None):
        self.server = server  # Tornado object
        self.host = host
        self.port = port


class Hello(RequestHandler):
    def get(self):
        response_code = 200
        response_status = "OK"
        reason = "Hello!"
        self.set_status(response_code, reason)
        response = {"code": response_code, "status": response_status, "message": reason}
        self.write(json.dumps(response))
        self.finish()


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(
            obj,
            (
                np.int_,
                np.intc,
                np.intp,
                np.int8,
                np.int16,
                np.int32,
                np.int64,
                np.uint8,
                np.uint16,
                np.uint32,
                np.uint64,
            ),
        ):
            return int(obj)
        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def create_response(
    req_handler,
    response_code=200,
    response_status="OK",
    reason="",
    additional_response={},
    encode_numpy=False,
):
    req_handler.set_status(response_code, reason)

    response = {"code": response_code, "status": response_status, "message": reason}
    combined_response = {**response, **additional_response}
    if encode_numpy:
        req_handler.write(json.dumps(combined_response, cls=NumpyEncoder))
    else:
        req_handler.write(json.dumps(combined_response))
    req_handler.finish()


def create_dir(folder):
    try:
        os.mkdir(folder)
    except FileExistsError:
        print("Directory {} already exist.".format(folder))


def represents_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False


class CheckTask:
    def __init__(self, sqliteConn, task_name):
        self.sqliteConn = sqliteConn
        self.task_name = task_name

    def check_running_task(self, request_handler):
        tasklist = select_all_tasks(self.sqliteConn)
        if len(tasklist) > 0:
            task = tasklist[-1]
            if task[1] == self.task_name and task[2] == 1:
                reason = "Already exists"
                status = 409
                add_reason = {
                    "reason info": "We cannot process your request because there is a task that still on process in the background"
                }
                create_response(
                    request_handler,
                    response_code=status,
                    reason=reason,
                    additional_response=add_reason,
                )
                return 1
        return 0
