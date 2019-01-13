from datetime import datetime

from airflow import models
from airflow.operators import python_operator, bash_operator

default_dag_args = {
    'start_date': datetime(2019, 1, 14),  # ジョブの開始日時
    'retries': 0,  # task失敗時のretryの回数
}


def decider(**kwargs):
    hoge_or_fuga = kwargs['dag_run'].conf['word']  # DAGを実行時に渡した引数を取得
    if hoge_or_fuga == 'hoge':
        return 'hoge_task'  # hogeであればtask_idがhoge_taskのものを実行
    elif hoge_or_fuga == 'fuga':
        return 'fuga_task'  # fugeであればtask_idがfuge_taskのものを実行
    else:
        return


with models.DAG(
        'branch_python_operator',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    task1 = python_operator.BranchPythonOperator(
        task_id='branch_python',
        provide_context=True,  # トリガーするときに引数を渡すのでTrueにする
        python_callable=decider  # 実行したいpythonの関数
    )

    hoge_task = bash_operator.BashOperator(
        task_id='hoge_task',
        bash_command='echo hoge'
    )

    fuga_task = bash_operator.BashOperator(
        task_id='fuga_task',
        bash_command='echo fuge'
    )

    task1 >> hoge_task
    task1 >> fuga_task