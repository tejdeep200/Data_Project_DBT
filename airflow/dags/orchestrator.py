from datetime import datetime, timezone
import os
import time

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState


@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
)
def orchestrate():

    # ---------------------------------------------------------
    # 1. TRIGGER DATABRICKS CDC JOB
    # ---------------------------------------------------------
    @task
    def ingest_cdc():

        # Connect to Databricks
        ws = WorkspaceClient(
            host=os.environ["DATABRICKS_HOST"],
            token=os.environ["DATABRICKS_TOKEN"],
        )

        # Trigger your Databricks Job
        job_trigger = ws.jobs.run_now(
            job_id=260859050658516
        )

        print(f"Databricks Job started. Run ID: {job_trigger.run_id}")

        # Keep checking the Databricks job
        while True:

            job_run = ws.jobs.get_run(job_trigger.run_id)

            print(
                f"Job run status: {job_run.state.life_cycle_state}, "
                f"result state: {job_run.state.result_state}"
            )

            # Check whether job has finished
            if job_run.state.life_cycle_state in [
                RunLifeCycleState.TERMINATED,
                RunLifeCycleState.SKIPPED,
                RunLifeCycleState.INTERNAL_ERROR,
            ]:

                # Job succeeded
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Databricks Job completed successfully!")
                    break

                # Job failed
                raise Exception(
                    f"Databricks Job failed with state: "
                    f"{job_run.state.result_state}"
                )

            # Wait 5 seconds before checking again
            time.sleep(5)

        return "CDC Ingestion Completed"

    # ---------------------------------------------------------
    # 2. CLEAN DBT TARGET
    # ---------------------------------------------------------
    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project/target"

    # ---------------------------------------------------------
    # 3. SOURCE FRESHNESS
    # ---------------------------------------------------------
    @task.bash
    def source_freshness():
        return (
            "cd /opt/airflow/walmart_project && "
            "dbt source freshness --profiles-dir ."
        )

    # ---------------------------------------------------------
    # 4. SILVER TECHNICAL
    # ---------------------------------------------------------
    silver_technical = BashOperator(
        task_id="silver_technical",
        bash_command=(
            "cd /opt/airflow/walmart_project && "
            "dbt run --select silver_t --profiles-dir ."
        ),
    )

    # ---------------------------------------------------------
    # 5. SILVER BUSINESS
    # ---------------------------------------------------------
    silver_business = BashOperator(
        task_id="silver_business",
        bash_command=(
            "cd /opt/airflow/walmart_project && "
            "dbt run --select silver_b --profiles-dir ."
        ),
    )

    # ---------------------------------------------------------
    # 6. SILVER BUSINESS TESTS
    # ---------------------------------------------------------
    silver_business_tests = BashOperator(
        task_id="silver_business_tests",
        bash_command=(
            "cd /opt/airflow/walmart_project && "
            "dbt test --select silver_b --profiles-dir ."
        ),
    )

    # ---------------------------------------------------------
    # 7. GOLD EPHEMERAL
    # ---------------------------------------------------------
    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        bash_command=(
            "cd /opt/airflow/walmart_project && "
            "dbt run --select path:models/gold/ephemeral --profiles-dir ."
        ),
    )

    # ---------------------------------------------------------
    # 8. GOLD DIMENSIONS / SCD TYPE 2 SNAPSHOTS
    # ---------------------------------------------------------
    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        bash_command=(
            "cd /opt/airflow/walmart_project && "
            "dbt snapshot --profiles-dir ."
        ),
    )

    # ---------------------------------------------------------
    # 9. GOLD FACT
    # ---------------------------------------------------------
    gold_facts = BashOperator(
        task_id="gold_facts",
        bash_command=(
            "cd /opt/airflow/walmart_project && "
            "dbt run --select path:models/gold/fact --profiles-dir ."
        ),
    )

    # ---------------------------------------------------------
    # EXECUTION ORDER
    # ---------------------------------------------------------
    (
        ingest_cdc()
        >> clean_target()
        >> source_freshness()
        >> silver_technical
        >> silver_business
        >> silver_business_tests
        >> gold_ephemeral
        >> gold_dimensions
        >> gold_facts
    )


orchestrate_dag = orchestrate()
