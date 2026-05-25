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

# # nb_01_bronze_ingest
# **Pure transformation notebook — no logging inside.**
# 
# Called by `Pipeline_Bronze` after `nb_log_start`.
# 
# **Responsibilities:**
# 1. Read raw source file from `Files/raw/`
# 2. Attach Bronze audit columns (`batch_id_created`, `ingested_datetime`, `source_file_name`)
# 3. Append to Bronze Delta table
# 4. Exit with row counts JSON → pipeline passes to `nb_log_end`
# 
# > ✅ No control table updates here. Pipeline owns the logging.

# MARKDOWN ********************

# ## 1. Parameters

# PARAMETERS CELL ********************

batch_id      = ''   # injected by Pipeline_Bronze
pipeline_name = ''   # injected by Pipeline_Bronze
source_file   = ''   # e.g. Files/raw/sales.csv
source_system = ''   # e.g. source_system

print(f'batch_id      : {batch_id}')
print(f'pipeline_name : {pipeline_name}')
print(f'source_file   : {source_file}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Imports

# CELL ********************

from pyspark.sql.functions import lit, current_timestamp
import json

spark.conf.set('spark.sql.session.timeZone', 'UTC')
print(f'Active DB : {spark.catalog.currentDatabase()}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Lookup Target Table from pipeline_control

# CELL ********************

config = spark.sql(f"""
    SELECT target_table
    FROM   pipeline_control
    WHERE  pipeline_name = '{pipeline_name}'
""").collect()

if not config:
    raise Exception(f'[nb_01] pipeline_name [{pipeline_name}] not found in pipeline_control.')

target_table = config[0]['target_table']
print(f'Target table : {target_table}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Read Raw Source

# CELL ********************

df_raw = (
    spark.read
    .format('csv')
    .option('header', 'true')
    .option('inferSchema', 'true')
    .load(source_file)
)

rows_read = df_raw.count()
print(f'Rows read : {rows_read}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Attach Bronze Audit Columns
# Bronze rule: **no transformation**. Only attach audit metadata.

# CELL ********************

df_bronze = (
    df_raw
    .withColumn('batch_id_created',  lit(batch_id))
    .withColumn('ingested_datetime', current_timestamp())
    .withColumn('source_file_name',  lit(source_file))
)

print(f'Schema after audit columns:')
df_bronze.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Write to Bronze Delta Table

# CELL ********************

df_bronze.write \
    .format('delta') \
    .mode('append') \
    .saveAsTable(target_table)

rows_written = rows_read   # all rows written in Bronze (no filtering)
print(f'\u2705 Written {rows_written} rows to [{target_table}]')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Exit with Row Counts
# Pipeline captures this JSON and passes `rows_read`, `rows_written` to `nb_log_end`.

# CELL ********************

exit_value = json.dumps({
    'rows_read'   : rows_read,
    'rows_written': rows_written,
    'rows_failed' : 0
})

mssparkutils.notebook.exit(exit_value)
print(f'nb_01_bronze_ingest complete. exit={exit_value}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
