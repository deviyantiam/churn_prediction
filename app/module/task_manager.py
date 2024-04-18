from sqlite3 import Error
import sqlite3
import os
import errno
import traceback
import logging

service_logger = logging.getLogger(__name__)


def validate_db_file(db_file):
    task_db_path = db_file
    service_logger.info("Path for task database {}".format(task_db_path))
    if not os.path.exists(os.path.dirname(task_db_path)):
        try:
            os.makedirs(os.path.dirname(task_db_path))
            service_logger.info("file is not exist; thus it is created.")
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                service_logger.error(
                    "race condition. error {error}. traceback {traceback}".format( error=exc, traceback=traceback.format_exc())
                )
                raise ValueError("race condition when creating task database.")
    return task_db_path


def create_connection(db_file):
    """create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file, check_same_thread=False)
    except Error as e:
        service_logger.error(
            "error during db creation. error {error}. traceback {traceback}".format( error=e, traceback=traceback.format_exc())
        )
    return conn


def select_all_tasks(conn, tasktype="tasks"):
    """
    Query all rows in the tasks table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    if tasktype == "tasks":
        cur.execute("SELECT * FROM api_tasks")
    rows = cur.fetchall()
    tasklist = []
    for row in rows:
        tasklist.append(row)
    cur.close()
    return tasklist


def select_task(conn, task_id, tasktype="tasks"):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM api_tasks WHERE id=?", (task_id,))
    rows = cursor.fetchall()
    conn.commit()
    cursor.close()
    return rows


def count_active_task(conn, tasktype="tasks"):
    cur = conn.cursor()
    if tasktype == "tasks":
        cur.execute("SELECT count(id) FROM api_tasks WHERE status=1")
    rows = cur.fetchone()[0]
    cur.close()
    return rows


def create_table(conn):
    """create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    sql_create_table = """
    CREATE TABLE IF NOT EXISTS api_tasks (
        id text PRIMARY KEY,
        type text NOT NULL,
        status integer NOT NULL,
        begin_date timestamp NOT NULL,
        end_date timestamp NOT NULL,
        messages text
    );
    """
    try:
        c = conn.cursor()
        c.execute(sql_create_table)
    except Error as e:
        service_logger.error(
            "error during table creation. error {error}. traceback {traceback}".format( error=e, traceback=traceback.format_exc())
        )


def update_task(
    conn,
    task_id,
    task_type,
    task_status,
    task_start_date,
    task_end_date,
    messages=None,
    types="insert",
    tasktype="tasks",
):
    cursor = conn.cursor()
    service_logger.debug("id: {} ({})".format(task_id, type(task_id)))
    service_logger.debug("type: {} ({})".format(task_type, type(task_type)))
    service_logger.debug("status: {} ({})".format(task_status, type(task_status)))
    service_logger.debug(
        "start_date: {} ({})".format(task_start_date, type(task_start_date))
    )
    service_logger.debug("end_date: {} ({})".format(task_end_date, type(task_end_date)))

    if types == "insert":
        if tasktype == "tasks":
            sqlite_query = """INSERT INTO api_tasks
                            (id, type, status, begin_date, end_date)
                                VALUES
                            (?, ?, ?, ?, ?)"""
        data_tuple = (
            str(task_id),
            str(task_type),
            int(task_status),
            (task_start_date),
            (task_end_date),
        )
    else:
        if tasktype == "tasks" and messages is not None:
            # update from waiting to running task
            sqlite_query = """ UPDATE api_tasks
                SET status = ?,
                    begin_date = ?,
                    end_date = ?,
                    messages = ?
                WHERE id = ?"""
            data_tuple = (
                task_status,
                task_start_date,
                task_end_date,
                messages,
                task_id,
            )
        else:
            sqlite_query = """ UPDATE api_tasks
                SET status = ?,
                    begin_date = ?,
                    end_date = ?
                WHERE id = ?"""
            data_tuple = (task_status, task_start_date, task_end_date, task_id)

    cursor.execute(sqlite_query, data_tuple)
    conn.commit()
    service_logger.debug(
        "Record inserted/updated successfully into api_tasks table: {}".format(
            cursor.rowcount
        )
    )
    service_logger.debug("sqlite detail: {}\n{}".format(sqlite_query, data_tuple))
    cursor.close()
