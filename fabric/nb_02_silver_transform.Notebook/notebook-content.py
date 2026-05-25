# Fabric notebook source

# METADATA ********************

# META {
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

# # nb_02_silver_transform
# **Pure transformation notebook — no logging inside.**

# Called by `Pipeline_Silver`.

# **Responsibilities:**
# 1. Auto-discover Bronze SUCCESS batches not yet processed in Silver
# 2. For each batch: lookup Bronze → cleanse → load Silver
# 3. Exit with aggregated row counts → pipeline passes to `nb_log_end`

# > ✅ No control table updates here. Pipeline owns the logging.

# > Silver processes **all pending batches** in one run. Each batch is atomic.

# MARKDOWN ********************

# ## 1. Parameters

# PARAMETERS CELL ********************

pipeline_name = ''   # injected by Pipeline_Silver e.g. silver_transform_sales
source_system = ''   # injected by Pipeline_Silver

print(f'pipeline_name : {pipeline_name}')
print(f'source_system : {source_system}')

# MARKDOWN ********************

# ## 2. Imports

# CELL ********************

from pyspark.sql.functions import col, trim, upper, current_timestamp, lit
import json

spark.conf.set('spark.sql.session.timeZone', 'UTC')
print(f'Active DB : {spark.catalog.currentDatabase()}')

# MARKDOWN ********************

# ## 3. Lookup Pipeline Config

# CELL ********************

config = spark.sql(f"""
    SELECT target_table, source_system
    FROM   pipeline_control
    WHERE  pipeline_name = '{pipeline_name}'
""").collect()

if not config:
    raise Exception(f'[nb_02] pipeline_name [{pipeline_name}] not found in pipeline_control.')

target_table  = config[0]['target_table']

# Convention: silver_transform_sales -> bronze_sales
bronze_table  = 'bronze_' + pipeline_name.replace('silver_transform_', '')

print(f'Bronze source : {bronze_table}')
print(f'Silver target : {target_table}')

# MARKDOWN ********************

# ## 4. Discover Unprocessed Bronze Batches
# Idempotent query — safe to re-run. Will never double-process a batch.

# CELL ********************

df_pending = spark.sql(f"""
    SELECT b.batch_id,
           b.rows_written AS bronze_rows
    FROM   batch_control b
    WHERE  b.layer        = 'BRONZE'
    AND    b.status       = 'SUCCESS'
    AND    b.pipeline_name IN (
        SELECT pipeline_name FROM pipeline_control
        WHERE  layer = 'BRONZE'
        AND    source_system = '{source_system}'
    )
    AND    NOT EXISTS (
        SELECT 1 FROM batch_control s
        WHERE  s.batch_id      = b.batch_id
        AND    s.layer         = 'SILVER'
        AND    s.pipeline_name = '{pipeline_name}'
    )
    ORDER BY b.batch_start_dts
""")

pending_batches = df_pending.collect()
print(f'\U0001f50d Unprocessed Bronze batches: {len(pending_batches)}')
df_pending.show(truncate=False)

# MARKDOWN ********************

# ## 5. Process Each Batch
# Each batch is independent and atomic. A failure in one batch does not block others.

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType
from datetime import datetime
import uuid

total_rows_read    = 0
total_rows_written = 0
total_rows_failed  = 0
failed_batches     = []


def register_silver_batch(batch_id, pipeline_name, source_system):
    now = datetime.utcnow()
    schema = StructType([
        StructField('batch_id',        StringType(),    False),
        StructField('pipeline_name',   StringType(),    False),
        StructField('layer',           StringType(),    False),
        StructField('source_system',   StringType(),    True),
        StructField('source_file',     StringType(),    True),
        StructField('status',          StringType(),    False),
        StructField('retry_count',     IntegerType(),   False),
        StructField('rows_read',       LongType(),      True),
        StructField('rows_written',    LongType(),      True),
        StructField('rows_failed',     LongType(),      True),
        StructField('error_message',   StringType(),    True),
        StructField('batch_start_dts', TimestampType(), True),
        StructField('batch_end_dts',   TimestampType(), True),
        StructField('created_dts',     TimestampType(), False),
        StructField('updated_dts',     TimestampType(), False),
    ])
    row = [(batch_id, pipeline_name, 'SILVER', source_system, None,
            'RUNNING', 0, None, None, None, None, now, None, now, now)]
    spark.createDataFrame(row, schema=schema) \
         .write.format('delta').mode('append').saveAsTable('batch_control')


for row in pending_batches:
    b_id = row['batch_id']
    print(f'\n▶ Processing batch: {b_id}')

    register_silver_batch(b_id, pipeline_name, source_system)

    try:
        # LOOKUP from Bronze filtered by batch_id
        df_bronze = spark.sql(f"""
            SELECT *
            FROM   {bronze_table}
            WHERE  batch_id_created = '{b_id}'
        """)
        rows_read = df_bronze.count()

        # TRANSFORM: apply cleansing rules
        # Replace the examples below with your actual business rules
        df_silver = (
            df_bronze
            .drop('ingested_datetime', 'source_file_name')
            .withColumn('audit_create_dts',        current_timestamp())
            .withColumn('audit_updated_dts',       current_timestamp())
            .withColumn('audit_source_system',     lit(source_system))
            .withColumn('audit_created_batch_id',  lit(b_id))
            .withColumn('audit_updated_batch_id',  lit(b_id))
            # String cleansing examples:
            # .withColumn('customer_name', trim(col('customer_name')))
            # .withColumn('status_code',   upper(trim(col('status_code'))))
        )

        # LOAD into Silver
        df_silver.write.format('delta').mode('append').saveAsTable(target_table)
        rows_written = df_silver.count()

        # Update batch_control for this Silver batch
        run_end = datetime.utcnow()
        spark.sql(f"""
            UPDATE batch_control
            SET    status        = 'SUCCESS',
                   rows_read     = {rows_read},
                   rows_written  = {rows_written},
                   rows_failed   = 0,
                   batch_end_dts = '{run_end}',
                   updated_dts   = '{run_end}'
            WHERE  batch_id      = '{b_id}'
            AND    layer         = 'SILVER'
            AND    pipeline_name = '{pipeline_name}'
        """)

        total_rows_read    += rows_read
        total_rows_written += rows_written
        print(f'  \u2705 {b_id} SUCCESS | rows_read={rows_read} rows_written={rows_written}')

    except Exception as e:
        err_safe = str(e).replace("'", "''")
        run_end = datetime.utcnow()
        spark.sql(f"""
            UPDATE batch_control
            SET    status        = 'FAILED',
                   error_message = '{err_safe}',
                   rows_failed   = {rows_read if 'rows_read' in dir() else 0},
                   batch_end_dts = '{run_end}',
                   updated_dts   = '{run_end}'
            WHERE  batch_id      = '{b_id}'
            AND    layer         = 'SILVER'
            AND    pipeline_name = '{pipeline_name}'
        """)
        total_rows_failed += 1
        failed_batches.append(b_id)
        print(f'  \u274c {b_id} FAILED | {str(e)}')

print(f'\n\U0001f4ca Silver done: read={total_rows_read} written={total_rows_written} failed_batches={len(failed_batches)}')

# MARKDOWN ********************

# ## 6. Exit with Row Counts

# CELL ********************

exit_value = json.dumps({
    'rows_read'    : total_rows_read,
    'rows_written' : total_rows_written,
    'rows_failed'  : total_rows_failed,
    'failed_batches': failed_batches
})

# Raise so Pipeline_Silver marks activity as failed and calls nb_log_end failure branch
if failed_batches:
    mssparkutils.notebook.exit(exit_value)
    raise Exception(f'Silver: {len(failed_batches)} batch(es) failed: {failed_batches}')

mssparkutils.notebook.exit(exit_value)
print(f'nb_02_silver_transform complete. exit={exit_value}')
