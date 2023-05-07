from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from datetime import datetime

@dag(start_date=datetime(2023, 1 , 1), schedule='@daily', catchup=False)
def test_dag():

    tasks = [BashOperator(task_id='task_{0}'.format(t), bash_command='sleep 60'.format(t)) for t in range(1, 4)]

    @task
    def task_8(data):
        print(data)
        return 'done'
    
    @task
    def task_50(data):
        print(data)

    tasks >> task_50(task_8(42))

test_dag()
