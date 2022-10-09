import logging
import uuid
from datetime import datetime
import itertools
from airflow import DAG
from airflow.kubernetes.secret import Secret
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

import uuid

secret_env_endpoint_url = Secret('env', 'ENDPOINT-URL', 'minio-creds', key='ENDPOINT-URL')
secret_env_access_key = Secret('env', 'ACCESS-KEY', 'minio-creds', key='ACCESS-KEY')
secret_env_secret_key = Secret('env', 'SECRET-KEY', 'minio-creds', key='SECRET-KEY')
secret_env_bucket_name = Secret('env', 'BUCKET-NAME', 'minio-creds', key='BUCKET-NAME')

routingAlgorithm = ['dijkstra', 'astar', 'CH']
interval = ['10','20','30','40','50']
tau = [1,1.25]
minGap = [1,1.5,2,2.5,3]



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

    for algo, m, t, i in itertools.product(routingAlgorithm, minGap, tau, interval):

        run_simulation = KubernetesPodOperator(
            task_id=f'run-simulation-algo{algo}interval{i}minGap{m}tau{t}',
            name=f'run-simulation-algo{algo}interval{i}minGap{m}tau{t}',
            secrets=[secret_env_endpoint_url, secret_env_access_key, secret_env_secret_key, secret_env_bucket_name],
            image_pull_policy='Always',
            namespace='airflow',
            image='dinozavyr/traficsim:latest',
            is_delete_operator_pod=True,
            do_xcom_push=False,
            cmds=["python3"],
            arguments=["traficsim/main.py", '--uuid={{ ti.xcom_pull("get_run_uuid") }}', f"--algo={algo}", f"--minGap={m}", f"--tau={t}", f"--interval{i}"],
            dag=dag)
        run_simulation.set_upstream(get_run_uuid)



if __name__ == "__main__":
    dag.cli()
