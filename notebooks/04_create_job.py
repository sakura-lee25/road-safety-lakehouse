# Databricks notebook source
# MAGIC %md
# MAGIC # 04 Orchestration: create the bronze -> silver -> gold job
# MAGIC
# MAGIC Defines the pipeline as code with the Databricks SDK: three notebook tasks, each
# MAGIC depending on the one before, so a failure in silver stops gold from running on bad data.
# MAGIC Run this notebook once; afterwards the job can be rerun or scheduled from Jobs & Pipelines.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()
base = f"/Users/{w.current_user.me().user_name}"
JOB_NAME = "road_safety_lakehouse"

steps = ["01_bronze_ingest", "02_silver_clean", "03_gold_star_schema"]
tasks = []
for i, nb in enumerate(steps):
    tasks.append(
        jobs.Task(
            task_key=nb,
            notebook_task=jobs.NotebookTask(notebook_path=f"{base}/{nb}"),
            depends_on=[jobs.TaskDependency(task_key=steps[i - 1])] if i else None,
        )
    )

existing = [j for j in w.jobs.list(name=JOB_NAME)]
if existing:
    job_id = existing[0].job_id
    w.jobs.reset(job_id=job_id, new_settings=jobs.JobSettings(name=JOB_NAME, tasks=tasks))
else:
    job_id = w.jobs.create(name=JOB_NAME, tasks=tasks).job_id
print("job id:", job_id)

# COMMAND ----------

run = w.jobs.run_now(job_id=job_id).result()  # waits until the run finishes
for t in run.tasks:
    print(t.task_key, t.state.result_state)
