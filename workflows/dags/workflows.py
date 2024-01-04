import textwrap
from datetime import datetime, timedelta

from airflow.models.dag import DAG

from airflow.operators.bash import BashOperator

with DAG(
    "eth_transactions_workflows",

    default_args={
        "depends_on_past": False,
        "email": ["aleksabajat2001@gmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="Simple DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["simple"],
    ) as dag:

    t1 = BashOperator(
            task_id="print_date",
            bash_command="date"
    )

    t1.doc_md = textwrap.dedent(
            """\
                    #### Task Documentation

                    Task that prints date!
            """
    )

    t2 = BashOperator(
        task_id="sleep",
        depends_on_past=False,
        bash_command="sleep 5",
        retries=3
    )

    dag.doc_md = __doc__

    t1 >> [t2]
