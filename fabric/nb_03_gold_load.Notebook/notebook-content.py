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

# # nb_03_gold_load
# **Pure transformation notebook — no logging inside.**

# Called by `Pipeline_Gold`.

# **Responsibilities:**
# 1. Auto-discover Silver SUCCESS batches not yet processed in Gold
# 2. For each batch: lookup Silver → apply dimensional/fact logic → MERGE into Gold
# 3. Exit with aggregated row counts → pipeline passes to `nb_log_end`

# > ✅ No control table updates here. Pipeline owns the logging.

# > Surrogate keys are generated at Gold layer only. MERGE handles SCD logic.

# MARKDOWN ********************

# ## 1. Parameters

# PARAMETERS CELL ********************

pipeline_name   = ''   # e.g. gold_fact_sales
silver_pipeline = ''   # e.g. silver_transform_sales
source_system   = ''   # injected by Pipeline_Gold

print(f'pipeline_name   : {pipeline_name}')
print(f'silver_pipeline : {silver_pipeline}')
print(f'source_system   : {source_system}')

# MARKDOWN ********************

# ## 2. Imports

# CELL ********************

from pyspark.sql.functions import current_timestamp, lit, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType
from delta.tables import DeltaTable
from datetime import datetime
import json
import uuid

spark.conf.set('spark.sql.session.timeZone', 'UTC')
print(f'Active DB : {spark.catalog.currentDatabase()}')

# MARKDOWN ********************

# ## 3. Lookup Pipeline Config

# CELL ********************

config = spark.sql(f"""
    SELECT target_table
    FROM   pipeline_control
    WHERE  pipeline_name = '{pipeline_name}'
""").collect()

if not config:
    raise Exception(f'[nb_03] pipeline_name [{pipeline_name}] not found in pipeline_control.')

target_table = config[0]['target_table']

# Convention: silver_transform_sales -> silver_sales
silver_table = 'silver_' + silver_pipeline.replace('silver_transform_', '')

print(f'Silver source : {silver_table}')
print(f'Gold target   : {target_table}')

# MARKDOWN ********************

# ## 4. Discover Unprocessed Silver Batches

# CELL ********************

df_pending = spark.sql(f"""
    SELECT s.batch_id,
           s.rows_written AS silver_rows
    FROM   batch_control s
    WHERE  s.layer         = 'SILVER'
    AND    s.status        = 'SUCCESS'
    AND    s.pipeline_name = '{silver_pipeline}'
    AND    NOT EXISTS (
        SELECT 1 FROM batch_control g
        WHERE  g.batch_id      = s.batch_id
        AND    g.layer         = 'GOLD'
        AND    g.pipeline_name = '{pipeline_name}'
    )
    ORDER BY s.batch_start_dts
""")

pending_batches = df_pending.collect()
print(f'\U0001f50d Unprocessed Silver batches: {len(pending_batches)}')
df_pending.show(truncate=False)

# MARKDOWN ********************

# ## 5. Process Each Batch

# CELL ********************

total_rows_read    = 0
total_rows_written = 0
total_rows_failed  = 0
failed_batches     = []


def register_gold_batch(batch_id, pipeline_name, source_system):
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
    row = [(batch_id, pipeline_name, 'GOLD', source_system, None,
            'RUNNING', 0, None, None, None, None, now, None, now, now)]
    spark.createDataFrame(row, schema=schema) \
         .write.format('delta').mode('append').saveAsTable('batch_control')


for row in pending_batches:
    b_id = row['batch_id']
    rows_read = 0
    print(f'\n▶ Processing batch: {b_id}')

    register_gold_batch(b_id, pipeline_name, source_system)

    try:
        # LOOKUP from Silver filtered by batch_id
        df_silver = spark.sql(f"""
            SELECT *
            FROM   {silver_table}
            WHERE  audit_created_batch_id = '{b_id}'
        """)
        rows_read = df_silver.count()

        # TRANSFORM: apply Gold dimensional/fact logic
        # Surrogate keys generated here at Gold layer only
        df_gold = (
            df_silver
            .withColumn('audit_create_dts',        current_timestamp())
            .withColumn('audit_updated_dts',       current_timestamp())
            .withColumn('audit_created_batch_id',  lit(b_id))
            .withColumn('audit_updated_batch_id',  lit(b_id))
            .withColumn('audit_source_system',     lit(source_system))
            # Surrogate key example (uncomment and customise):
            # .withColumn('product_key', monotonically_increasing_id())
        )

        # MERGE into Gold (upsert for dimensions, append for facts)
        table_exists = spark.catalog.tableExists(target_table)

        if not table_exists:
            # Initial load
            df_gold.write.format('delta').mode('overwrite').saveAsTable(target_table)
            print(f'  [Gold] Initial load into [{target_table}]')
        else:
            # Dimension: use MERGE by natural key for SCD Type 1
            # Uncomment and set your natural_key column:
            # DeltaTable.forName(spark, target_table).alias('tgt') \
            #     .merge(df_gold.alias('src'), 'tgt.natural_key = src.natural_key') \
            #     .whenMatchedUpdateAll() \
            #     .whenNotMatchedInsertAll() \
            #     .execute()

            # Fact: append only
            df_gold.write.format('delta').mode('append').saveAsTable(target_table)

        rows_written = df_gold.count()

        # Update batch_control for this Gold batch
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
            AND    layer         = 'GOLD'
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
                   rows_failed   = {rows_read},
                   batch_end_dts = '{run_end}',
                   updated_dts   = '{run_end}'
            WHERE  batch_id      = '{b_id}'
            AND    layer         = 'GOLD'
            AND    pipeline_name = '{pipeline_name}'
        """)
        total_rows_failed += 1
        failed_batches.append(b_id)
        print(f'  \u274c {b_id} FAILED | {str(e)}')

print(f'\n\U0001f4ca Gold done: read={total_rows_read} written={total_rows_written} failed_batches={len(failed_batches)}')

# MARKDOWN ********************

# ## 6. Exit with Row Counts

# CELL ********************

exit_value = json.dumps({
    'rows_read'     : total_rows_read,
    'rows_written'  : total_rows_written,
    'rows_failed'   : total_rows_failed,
    'failed_batches': failed_batches
})

if failed_batches:
    mssparkutils.notebook.exit(exit_value)
    raise Exception(f'Gold: {len(failed_batches)} batch(es) failed: {failed_batches}')

mssparkutils.notebook.exit(exit_value)
print(f'nb_03_gold_load complete. exit={exit_value}')
