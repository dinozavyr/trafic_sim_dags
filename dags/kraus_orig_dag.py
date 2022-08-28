import logging
import uuid
from datetime import datetime
import itertools
from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

import uuid

secret_env_auth_url = Secret('env', 'OS_AUTH_URL', 'os-creds', key='OS_AUTH_URL')
secret_env_id = Secret('env', 'OS_APPLICATION_CREDENTIAL_ID', 'os-creds', key='OS_APPLICATION_CREDENTIAL_ID')
secret_env_secret = Secret('env', 'OS_APPLICATION_CREDENTIAL_SECRET', 'os-creds', key='OS_APPLICATION_CREDENTIAL_SECRET')

routingAlgorithm = ['dijkstra', 'astar', 'CH']
minGap = [1, 1.5, 2, 2.5, 3]
tau = [1, 1.25]

with DAG(
        dag_id='kraus_orig_dag',
        schedule_interval=None,
        start_date=datetime(2022, 1, 1),
        catchup=False,
        max_active_tasks=12,
        tags=['traficsim'],
) as dag:
    get_run_uuid = PythonOperator(
        task_id='get_run_uuid',
        python_callable=uuid.uuid4,
        dag=dag)

    for algo, m, t in itertools.product(routingAlgorithm, minGap, tau):

        run_simulation = KubernetesPodOperator(
            task_id=f'run_simulation_algo_{algo}_minGap_{m}_tau_{t}',
            name=f'run_simulation_algo_{algo}_minGap_{m}_tau_{t}',
            secrets=[secret_env_auth_url, secret_env_id, secret_env_secret],
            image_pull_policy='Always',
            namespace='airflow',
            image='dinozavyr/traficsim:latest',
            is_delete_operator_pod=True,
            do_xcom_push=False,
            cmds=["python3"],
            arguments=["traficsim/main.py", '--uuid={{ ti.xcom_pull("get_run_uuid") }}', f"--algo={algo}", f"--minGap={m}", f"--tau={t}"],
            dag=dag)
        run_simulation.set_upstream(get_run_uuid)



if __name__ == "__main__":
    dag.cli()
