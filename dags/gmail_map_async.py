# import os, logging
# from datetime import datetime, date
# from tempfile import NamedTemporaryFile
# from airflow.sdk import dag, task, chain, Variable
# from airflow.exceptions import AirflowSkipException

# # Logger for logging the details
# task_logger = logging.getLogger("airflow.task")



# @dag(start_date=datetime(2025,8,26), schedule="@weekly", catchup=True)
# def gmail_mapped_async() -> None:        
#     @task
#     def get_ids() -> list[str]:
#         """ 
#             Gets the Email ids of last 7 days. 
#         """
#         from utils.gm_single_utils import get_dates, async_get_ids, wrapper_get_single_main        
#         token_path = Variable.get("TOKEN_PATH")
        
#         from_date, num_of_days = date.today(), 7
#         dates = get_dates(from_date, num_of_days)
#         ids_list = wrapper_get_single_main(func = async_get_ids,
#                                            a_list = dates,
#                                            token_path = token_path,
#                                            coro_num = 7,
#                                            for_ids = True) 
#         return ids_list


