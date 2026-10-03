import os
from databricks.sdk import WorkspaceClient


ws = WorkspaceClient(
    host=os.environ[DATABRICKS_HOST],
    token=os.environ[DATABRICKS_TOKEN],
)

job_trigger = ws.jobs.run_now(
    job_id=260859050658516
)

print(job_trigger)