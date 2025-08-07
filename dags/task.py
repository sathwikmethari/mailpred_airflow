import os, datetime, collections
from functools import partial
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from airflow.sdk import dag, task, chain
from typing import List

@dag
def email_data():
    @task
    def get_ids() -> List[int]:
        """Importing libraries."""
        from imapclient import IMAPClient
        
        """Getting email and password."""
        email = os.environ.get("user")
        password = os.environ.get("pass")
        from_date = datetime.date(2025, 8, 1)
        
        server = IMAPClient('imap.gmail.com', ssl=True, use_uid=True)
        server.login(email, password)
        server.select_folder('INBOX')
        ids = server.search(criteria=[u'SINCE', from_date])
        server.logout()
        return ids
    
    _my_task_1 = get_ids()
    
    @task
    def get_mail_data(list_of_ids : list[int]) -> None:
        """Importing libraries."""
        from utils import fetch_batch       
        
        """Getting email and password."""
        email = os.environ.get("user")
        password = os.environ.get("pass")
        
        batch_size = 50          #Get better 
        id_chunks = [list_of_ids[i:i+batch_size] for i in range(0, len(list_of_ids), batch_size)]
        
        partial_function = partial(fetch_batch, email, password)
        
        with ThreadPoolExecutor(max_workers=7) as executor:   #multiple workers for faster calling/extracting
            results = list(executor.map(partial_function, id_chunks))
        
        out_dict=collections.defaultdict(list) #dict to store the relevant information
    
        for ele in results:          #results ===> list of dicts containing ids as keys, dicts of info as values.
            for x, y in ele.items(): #ele.items() ==> iterable(id of class str, info of class dict)
                out_dict['ids'].append(x)
                out_dict['RFC822'].append(y[b'RFC822'])
                out_dict['ENVELOPE'].append(y[b'ENVELOPE'])
        
        df = pd.DataFrame(out_dict)
        df.to_pickle(f"/opt/airflow/data/{datetime.datetime.now().strftime('%d-%m-%Y-%M-%H-%S')}.pkl")
        return

    
    _my_task_2 = get_mail_data(list_of_ids=_my_task_1)
    

    
    chain(
    _my_task_1,
    _my_task_2,
    )
    

email_data()


"""
Standard libs (`datetime`, `functools`) -> At top of DAG file -> Cleaner, faster, shared.
Common libs (`pandas`, `numpy`)  -> At top of DAG file -> Avoid duplication.
Rare, heavy, optional libs (`torch`, `cv2`) -> Inside task  -> Avoid parse-time errors.
"""
