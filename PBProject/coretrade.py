from datetime import timedelta
import datetime
from airflow import DAG
from airflow.utils import dates
from airflow.utils.helpers import chain
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# 定义dag的缺省参数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2020-06-20',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# 定义DAG
coretradedag = DAG(
    'coretrade',
    default_args=default_args,
    description='core trade analyze',
    schedule_interval='30 0 * * *',
)

today=datetime.date.today()
oneday=timedelta(days=1)
yesterday=(today-oneday).strftime("%Y-%m-%d")


odstask = BashOperator(
    task_id='ods_load_data',
    depends_on_past=False,
    bash_command='sh /data/lagoudw/script/core_trade/ods_load_trade.sh ' + yesterday,
    dag=coretradedag
)


dimtask1 = BashOperator(
    task_id='dimtask_product_cat',
    depends_on_past=False,
    bash_command='sh /data/lagoudw/script/core_trade/dim_load_product_cat.sh ' + yesterday,
    dag=coretradedag
)

dimtask2 = BashOperator(
    task_id='dimtask_shop_org',
    depends_on_past=False,
    bash_command='sh /data/lagoudw/script/core_trade/dim_load_shop_org.sh ' + yesterday,
    dag=coretradedag
)

dimtask3 = BashOperator(
    task_id='dimtask_payment',
    depends_on_past=False,
    bash_command='sh /data/lagoudw/script/core_trade/dim_load_payment.sh ' + yesterday,
    dag=coretradedag
)

dimtask4 = BashOperator(
    task_id='dimtask_product_info',
    depends_on_past=False,
    bash_command='sh /data/lagoudw/script/core_trade/dim_load_product_info.sh ' + yesterday,
    dag=coretradedag
)

dwdtask = BashOperator(
    task_id='dwd_load_data',
    depends_on_past=False,
    bash_command='sh /data/lagoudw/script/core_trade/dwd_load_trade_orders.sh ' + yesterday,
    dag=coretradedag
)

dwstask = BashOperator(
    task_id='dws_load_data',
    depends_on_past=False,
    bash_command='sh /data/lagoudw/script/core_trade/dws_load_trade_orders.sh ' + yesterday,
    dag=coretradedag
)

adstask = BashOperator(
    task_id='ads_load_data',
    depends_on_past=False,
    bash_command='sh /data/lagoudw/script/core_trade/ads_load_trade_order_analysis.sh ' + yesterday,
    dag=coretradedag
)


odstask >> dimtask1
odstask >> dimtask2
odstask >> dimtask3
odstask >> dimtask4
odstask >> dwdtask

dimtask1 >> dwstask
dimtask2 >> dwstask
dimtask3 >> dwstask
dimtask4 >> dwstask
dwdtask >> dwstask

dwstask >> adstask
