from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Xác định các tham số cơ bản của DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Định nghĩa một DAG với tên là "example_dag"
dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),  # Lập lịch chạy hàng ngày
)

# Định nghĩa các Task trong DAG
task_1 = BashOperator(
    task_id='task_1',
    bash_command='echo "Hello from Task 1"',
    dag=dag,
)

task_2 = BashOperator(
    task_id='task_2',
    bash_command='echo "Hello from Task 2"',
    dag=dag,
)

task_3 = BashOperator(
    task_id='task_3',
    bash_command='echo "Hello from Task 3"',
    dag=dag,
)

# Xác định các quy tắc luồng dữ liệu giữa các Task
task_1 >> task_2 >> task_3
