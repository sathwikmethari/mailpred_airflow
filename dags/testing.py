from airflow.sdk import dag, task, chain

@dag
def testing():
    @task
    def test():
        with open("/opt/airflow/data/test.txt", "w") as f:
            f.write("hello airflow")
        print("✅ File written to /opt/airflow/data/test.txt")
        
    chain(test())

testing()