import os
import time
from datetime import timedelta
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


@dag(schedule=None, catchup=False, max_active_runs=1)
def orchestrate():

    @task(execution_timeout=timedelta(hours=2))
    def ingest_cdc():
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

        required = ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_JOB_ID")
        missing = [name for name in required if not os.environ.get(name, "").strip()]
        if missing:
            raise ValueError(f"Missing environment variables: {', '.join(missing)}")

        try:
            job_id = int(os.environ["DATABRICKS_JOB_ID"])
        except ValueError as exc:
            raise ValueError("DATABRICKS_JOB_ID must be a positive integer") from exc
        if job_id <= 0:
            raise ValueError("DATABRICKS_JOB_ID must be a positive integer")

        ws = WorkspaceClient(
            host=os.environ["DATABRICKS_HOST"],
            token=os.environ["DATABRICKS_TOKEN"],
        )

        job_trigger = ws.jobs.run_now(job_id=job_id)

        while True:

            job_run = ws.jobs.get_run(job_trigger.run_id)

            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully!")
                    break
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")

            time.sleep(5)  # Wait for 5 seconds before checking the job status again

        return "CDC Ingestion Completed"

    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"

    @task.bash
    def source_freshness():
        # Manually set the working directory using the 'cd' command before running the dbt command
        return "cd /opt/airflow/walmart_project && dbt source freshness"


    silver_technical = BashOperator(
        task_id='silver_technical',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select silver_t'
    )

    silver_technical_tests = BashOperator(
        task_id='silver_technical_tests',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt test --select silver_t'
    )

    silver_business = BashOperator(
            task_id='silver_business',
            cwd='/opt/airflow/walmart_project',
            bash_command='dbt run --select silver_b'
        )

    silver_business_tests = BashOperator(
        task_id='silver_business_tests',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt test --select silver_b'
    )

    gold_dimensions = BashOperator(
        task_id='gold_dimensions',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt snapshot'
    )

    gold_facts = BashOperator(
        task_id='gold_facts',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select path:models/gold/fact'
    )

    gold_tests = BashOperator(
        task_id='gold_tests',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt test --select path:models/gold'
    )

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests  >> silver_business >> silver_business_tests >> gold_dimensions >> gold_facts >> gold_tests

orchestrate_dag = orchestrate()
