# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c0f689d9-8f35-4699-b478-949faa2c5870",
# META       "default_lakehouse_name": "source",
# META       "default_lakehouse_workspace_id": "ccb26ac4-9887-4f7d-892e-2d3d972b14ff",
# META       "known_lakehouses": [
# META         {
# META           "id": "c0f689d9-8f35-4699-b478-949faa2c5870"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_log_end
# **Called by:** All Fabric Pipelines at the END of every run — on **both success and failure** branches.
# 
# Updates:
# - `batch_control` — final status, row counts, duration, error message
# - `pipeline_run_log` — updates the RUNNING entry to SUCCESS or FAILED
# 
# > Pipeline must call this on **both** success and failure paths.
# > This guarantees every run is always fully logged, even if the transform crashes.

# MARKDOWN ********************

# ## 1. Parameters
# Injected by Fabric Pipeline at runtime.

# PARAMETERS CELL ********************

batch_id       = ''       # same batch_id passed to nb_log_start
pipeline_name  = ''       # must match pipeline_control.pipeline_name
layer          = ''       # BRONZE | SILVER | GOLD
source_system  = ''       # source identifier
status         = ''       # SUCCESS | FAILED
rows_read      = 0        # from transform notebook exit value
rows_written   = 0        # from transform notebook exit value
rows_failed    = 0        # from transform notebook exit value
error_message  = ''       # from pipeline error expression (empty string if success)
run_start_str  = ''       # ISO string captured from nb_log_start exit value

print(f'batch_id      : {batch_id}')
print(f'pipeline_name : {pipeline_name}')
print(f'layer         : {layer}')
print(f'status        : {status}')
print(f'rows_written  : {rows_written}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Imports

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, LongType
)
from datetime import datetime

spark.conf.set('spark.sql.session.timeZone', 'UTC')
run_end = datetime.utcnow()

# Parse run_start from string passed by pipeline
try:
    run_start = datetime.fromisoformat(run_start_str)
    duration  = int((run_end - run_start).total_seconds())
except Exception:
    run_start = None
    duration  = None

# Sanitise inputs
rows_read     = int(rows_read)    if rows_read    else 0
rows_written  = int(rows_written) if rows_written else 0
rows_failed   = int(rows_failed)  if rows_failed  else 0
err_safe      = (str(error_message) or '').replace("'", "''")

print(f'run_end  : {run_end}')
print(f'duration : {duration}s')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Update `batch_control`

# CELL ********************

spark.sql(f"""
    UPDATE batch_control
    SET    status        = '{status}',
           rows_read     = {rows_read},
           rows_written  = {rows_written},
           rows_failed   = {rows_failed},
           error_message = '{err_safe}',
           batch_end_dts = '{run_end}',
           updated_dts   = '{run_end}'
    WHERE  batch_id      = '{batch_id}'
    AND    layer         = '{layer}'
    AND    pipeline_name = '{pipeline_name}'
""")

print(f'[batch_control] Updated: {batch_id} | {layer} | {status}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Update `pipeline_run_log`
# Find the RUNNING entry for this batch and update it to final status.

# CELL ********************

# Lookup notebook_name
meta = spark.sql(f"""
    SELECT notebook_name
    FROM   pipeline_control
    WHERE  pipeline_name = '{pipeline_name}'
""").collect()
notebook_name = meta[0]['notebook_name'] if meta else 'unknown'

# Update the RUNNING log entry for this batch+layer+pipeline
spark.sql(f"""
    UPDATE pipeline_run_log
    SET    status           = '{status}',
           run_end_dts      = '{run_end}',
           duration_seconds = {duration if duration is not None else 'NULL'},
           rows_read        = {rows_read},
           rows_written     = {rows_written},
           rows_failed      = {rows_failed},
           error_message    = '{err_safe}'
    WHERE  batch_id         = '{batch_id}'
    AND    layer            = '{layer}'
    AND    pipeline_name    = '{pipeline_name}'
    AND    status           = 'RUNNING'
""")

print(f'[pipeline_run_log] Updated: {batch_id} | {layer} | {status} | duration={duration}s | rows_written={rows_written}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Exit

# CELL ********************

mssparkutils.notebook.exit(f'{status}|rows_written={rows_written}|duration={duration}s')
print('nb_log_end complete.')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
