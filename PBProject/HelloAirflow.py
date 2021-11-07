from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils import dates
from airflow.utils.helpers import chain
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator


def default_options():
    default_args = {
        'owner': 'airflow',
        'start_date': dates.days_ago(1),
        'retries': 1,
        'retry_delay': timedelta(seconds=5)
    }
    return default_args

# 定义DAG
def task1(dag):
    t = "pwd"
    # operator支持多种类型，这里使用 BashOperator
    task = BashOperator(
        task_id='MyTask1',
        bash_command=t,
        dag=dag
    )
    return task


def hello_world():
    current_time = str(datetime.today())
    print('hello world at {}'.format(current_time))


def task2(dag):
    # Python Operator
    task = PythonOperator(
        task_id='MyTask2',
        python_callable=hello_world,
        dag=dag
    )
    return task


def task3(dag):
    t = "date"
    task = BashOperator(
        task_id='MyTask3',
        bash_command=t,
        dag=dag
    )
    return task


with DAG(
        'HelloWorldDag',
        default_args=default_options(),
        schedule_interval="*/2 * * * *"
) as d:
    task1 = task1(d)
    task2 = task2(d)
    task3 = task3(d)
    chain(task1, task2, task3)
