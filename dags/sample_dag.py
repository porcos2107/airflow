from datetime import datetime

from airflow import models
from airflow.operators import bash_operator


default_dag_args = {
    'start_date': datetime(2019, 1, 14),  # ジョブの開始日時
    'retries': 0,  # task失敗時のretryの回数
}

with models.DAG(
        'bashoperator_demo',
        schedule_interval=None,  # cron形式で記述するscheduleの実行時間（今回はスケジュールを設定しない）
        default_args=default_dag_args) as dag:

    task1 = bash_operator.BashOperator(
        task_id='echo',  # taskの識別子
        bash_command="echo Start task"
    )

    task2 = bash_operator.BashOperator(
        task_id='sleep',
        bash_command='sleep 5'
    )

    task1 >> task2  # operatorインスタンス間の依存関係を記述
