import logging

def make_logger():
    return logging.getLogger("airflow.task")