# Databricks notebook source
# MAGIC %md
# MAGIC | | |
# MAGIC |---|---|
# MAGIC |Description: |This notebook demonstrates the use of the Snowflake Data Tool for migrating tables into Databricks|

# COMMAND ----------

# DBTITLE 1,Import Snowflake Tools
# MAGIC %run ./Utils/class_SnowflakeDataTool

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Migrate data from Snowflake into Delta Tables

# COMMAND ----------

# DBTITLE 1,Instantiate Snowflake Tool
sfdt = SnowflakeDataTool(connectionName='dev')

# COMMAND ----------

# DBTITLE 1,Filter for Relevant Tables
#tables_list = sfdt.list_tables(includes=['interesting'], excludes=['boring', 'useless'])
tables_list = ['sfdb.table_a', 'sfdb.table_b', 'sfdb.table_c']
snowflake_tables = [t.split(".")[1] for t in tables_list]
snowflake_tables

# COMMAND ----------

# DBTITLE 1,Migrate Full Snowflake Tables
for table in snowflake_tables:
    query = f"SELECT * FROM {table}"    # Build query
    df = sfdt.sql(query)    # Retrieve data from Snowflake
    sfdt.write_to_delta(df, f"{table}_full", mode="overwrite")  # Drop table into Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Generate cleaned and filtered tables to use in analysis

# COMMAND ----------

# DBTITLE 1,Defining Variables and Helper Functions
database = "new_db"
snowflake_table = 'test_table'

# Common metadata columns that be will dropped as they are not necessary for analysis
drops = ['source_date','file_path','ELT_Time','__date']
clean_tb_name = lambda tb: f'{database}.{tb.replace("TEST","CLEANED")}'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning steps: Snowflake_Table
# MAGIC ### Column Drops
# MAGIC * **COLUMN1**
# MAGIC   * There is no variance in this column
# MAGIC   * Not helpful in analysis
# MAGIC * **COLUMN2**
# MAGIC   * There is no variance in this column
# MAGIC   * Not helpful in analysis
# MAGIC 
# MAGIC ### Data Filters
# MAGIC * Rows where **INTEREST_LVL** contains **_"High"_**
# MAGIC 
# MAGIC ### Transformations
# MAGIC * Cast the following decimal columns as integer types:  
# MAGIC   * **FUN_SCORE**

# COMMAND ----------

# DBTITLE 1,Filter and Clean (test_table)
additional_drops = drops.copy()
additional_drops.insert(0, 'column1')
additional_drops.insert(0, 'column2')

data_df = spark.sql(f"SELECT * FROM {database}.{table}")

# Drop inessential columns
for d in additional_drops:
    data_df = data_df.drop(d)

# Data filters
data_df = data_df.filter(col('INTEREST_LVL').contains('High'))

# Format integer type values
integer_cols = ['FUN_SCORE']
for ic in integer_cols:
    data_df = data_df.withColumn(ic, col(ic).astype(LongType()))

# Write cleaned table to a delta table
table = clean_tb_name(snowflake_table)
data_df.write.saveAsTable(table, format='delta', mode='overwrite')
_ = spark.sql(f"OPTIMIZE {table}")
