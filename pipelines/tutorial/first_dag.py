from datetime import datetime, timedelta
from textwrap import dedent

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.operators.bash import EmptyOperator

from utils.callbacks import failure_callback, success_callback

local_timezone = pendulum.timezone("Asia/Seoul")

with DAG(
    # 실습: "simple_dag"이라는 이름의 DAG 설정
    dag_id="simple_dag",
    # 실습: default_args에는 다음 내용이 들어감
    # 실습: "user" 사용자가 소유한 DAG / 본인의 이메일 / 실패 및 재시도 시 이메일 알림 여부
    # 실습: 재시도 1회 / 재시도 간격 5분
    # 실습: 실패 시 callback (failure_callback) / 성공 시 callback (success_callback)
    default_args={
        "owner": "user",
        "depends_on_past": False, #네트워크가 연결되어있을 때만 의미가 있음
        "email": "821jys@gmail.com",
        "email_on_failure": False,#네트워크가 연결되어있을 때만 의미가 있음
        "email_on_retry": False,#네트워크가 연결되어있을 때만 의미가 있음
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": failure_callback,
        "on_success_callback": success_callback,
    },
    description="Simple airflow dag",
    schedule="0 15 * * *",
    start_date=datetime(2025, 3, 1, tzinfo=local_timezone),
    catchup=False,
    tags=["lgcns", "mlops"], #dag가 굉장히 많은데 필요한것만 볼 수 있어서 좋음
) as dag:
    task1 = BashOperator(
        task_id="print_date",
        bash_command="date",
        # 실습: 현재 시간을 출력하는 bash_command 입력
    )
    task2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 5",
        retries=3,
        # 실습: 5초 sleep하는 bash_command를 입력하고 3회 재시도하도록 설정
    )

    loop_command = dedent(
        """
        {% for i in range(5) %}
            echo "ds = {{ ds }}"
            echo "macros.ds_add(ds, {{ i }}) = {{ macros.ds_add(ds, i) }}"
        {% endfor %}
        """
    )
    task3 = BashOperator(
        task_id="print_with_loop",
        bash_command=loop_command,
    )

    task1 >> [task2, task3]
