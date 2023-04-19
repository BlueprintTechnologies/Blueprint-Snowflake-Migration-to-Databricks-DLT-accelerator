import pytz

from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import *
from typing import Union

# COMMAND ----------

# DBTITLE 1,Snowflake Connections Class
class SnowflakeConnection:
    #----- PUBLIC METHODS -----#
    @classmethod
    def build_connection(cls, 
                         sfServer: str, 
                         sfDatabase: str, 
                         sfSchema: str, 
                         sfWarehouse: str, 
                         sfRole: str) -> dict:
        """Function used to build and retrieve Snowflake connection options"""
        
        connection_parameters = {
            "sfUrl": sfServer,
            "sfUser": "user",
            "sfPassword": "password",
            "sfDatabase": sfDatabase,
            "sfSchema": sfSchema,
            "sfWarehouse": sfWarehouse,
            "sfRole": sfRole
        }
        
        return connection_parameters
    
    @classmethod
    def get_connection(cls, connectionName:str):
        """Function used to retrieve pre-configured Snowflake connection options"""
        connection_parameters = {'dev':cls._dev(),
                                 'prod':cls._prod()}[connectionName]
        
        return connection_parameters
        
    #----- PRIVATE METHODS -----#
    @staticmethod
    def _dev():
        connOptions = {
            "sfUrl": 'https://xyz.snowflakecomputing.com',
            "sfUser": "user",
            "sfPassword": "password",
            "sfDatabase": "DB_Name",
            "sfSchema": "schema_value",
            "sfWarehouse": "warehouse_value"
        }
        return connOptions
    
    @staticmethod
    def _prod():
        connOptions = {
            "sfUrl": 'https://xyz.snowflakecomputing.com',
            "sfUser": "user",
            "sfPassword": "password",
            "sfDatabase": "DB_Name",
            "sfSchema": "schema_value",
            "sfWarehouse": "warehouse_value"
        }
        return connOptions

# COMMAND ----------

# DBTITLE 1,Data Tool Class Definition
"""
Created on Monday, October 31, 2022 at 09:24 by Adrian Tullock <atullock@bpcs.com>
Copyright (C) 2022, by Blueprint Technologies. All Rights Reserved.
"""
class SnowflakeDataTool:
    def __init__(self, sfServer: str=None, 
                 sfDatabase: str, 
                 sfSchema: str=None, 
                 sfWarehouse: str=None,
                 sfRole: str=None,
                 connectionName: str=None
                ):
        """
        --Parameters--
        sfServer: The Snowflake server to access
        sfDatabase: Set the Snowflake database to use
        sfSchema: Set the Snowflake schema to use in queries
        sfWarehouse: Set the warehouse to use in Snowflake
        sfRole: Set the role to use in Snowflake
        connectionName: Name of a pre-configured connection
        """
        # List of tables
        self._sfTables = []
        
        self._sfConnection = None
        # Snowflake connection object
        if connectionName is not None:
            self._connectionOptions = SnowflakeConnection.get_connection(connectionName)
            self.config_connect()
        else:
            self._connection_options = None
            self.connect(sfServer=sfServer, sfDatabase=sfDatabase, sfSchema=sfSchema, sfWarehouse=sfWarehouse, sfRole=sfRole)
        
        
    #----- PUBLIC METHODS -----#
    def config_connect(self):
        """Connect to the Snowflake database"""
        try:
            connObject = spark.read.format("snowflake").options(**self._connectionOptions)
        except Exception as e:
            raise e
            
        self._sfConnection = connObject
        
        if not self.is_connected():
            print("Failed to establish a connection. Check your connection options.")
        else:
            self._query_table_list()
        
        return None
    
    def connect(self, 
                sfServer: str, 
                sfDatabase: str, 
                sfSchema: str, 
                sfWarehouse: str,
                sfRole: str
               ):
        """Connect to the Snowflake database
        --Parameters--
        sfServer: The Snowflake server to access
        sfDatabase: Set the Snowflake database to use
        sfSchema: Set the Snowflake schema to use in queries
        sfWarehouse: Set the warehouse to use in Snowflake
        sfRole: Set the role to use in Snowflake
        """
        self._connection_options = SnowflakeConnection.build_connection(sfServer, sfDatabase, sfSchema, sfWarehouse, sfRole)
        connObject = spark.read.format("snowflake").options(**self._connection_options)
        self._sfConnection = connObject
            
        if not self.is_connected():
            print("Failed to establish a connection. Check your connection options.")
        else:
            self._query_table_list()
        
        return None
      
    def get_database_name(self):
        return self._connectionOptions.get("sfDatabase")
    
    def get_schema_name(self):
        return self._connectionOptions.get("sfSchema")
    
    def is_connected(self):
        """Verify if connected to Snowflake database"""
        try:
            query = "SELECT current_version()"
            _ = self._sfConnection.option("query",query).load()
            status = True
        except Exception:
            status = False
            
        return status
    
    def list_tables(self, includes: Union[str, list]=None, excludes: Union[str, list]=None, view_all=False):
        """View the list of tables in the database"""
        if view_all:
            tables_list = self._sfTables
        else:
            # Filter results by declared schema
            tables_list = [t for t in self._sfTables if t.startswith(self.get_schema_name())]
            
            # Filter results (includes)
            if type(includes) is str:
                tables_list = [t for t in tables_list if t.find(includes.upper()) >= 0]
            if type(includes) is list:
                for i in includes:
                    tables_list = [t for t in tables_list if t.find(i.upper()) >= 0]
                    
            # Filter results (excludes)
            if type(excludes) is str:
                tables_list = [t for t in tables_list if t.find(excludes.upper()) < 0]
            if type(excludes) is list:
                for ex in excludes:
                    tables_list = [t for t in tables_list if t.find(ex.upper()) < 0]
            
        return tables_list
    
    def query_column_metadata(self, tableName):
        """Retrieve a table's (tableName) column metadata"""
        OP = 'ORDINAL_POSITION'
        columns = ['COLUMN_NAME','DATA_TYPE',OP,'IS_NULLABLE']
        table = tableName.upper()
        sqlQuery = f"SELECT {','.join(columns)} FROM information_schema.columns WHERE table_name = '{table}' ORDER BY {OP}"
        column_data_df = self.sql(sqlQuery)
        
        return column_data_df
    
    def sql(self, query:str):
        """Execute a SQL query against the database"""
        try:
            results = self._sfConnection.option("query", query).load()
        except Exception as e:
            raise
                
        return results
    
    def write_to_delta(self, df, tableName: str, databaseName: str='stf_db', mode: str='append'):
        """Write data to a Delta table"""
        # Append metadata
        df = self._append_metadata(df)
        
        if not spark.catalog.databaseExists(databaseName):
            print(f"Creating database: {databaseName}")
            spark.sql(f"CREATE DATABASE {databaseName}")
            
        spark.catalog.setCurrentDatabase(databaseName)
        table = f"{databaseName}.{tableName}".lower()
        
        result = df.write.saveAsTable(table, format='delta', mode=mode, partitionBy=['__date'])
        _ = spark.sql(f"OPTIMIZE {table}")
        
        print(f"{table}: write successful!")
        return result
    
    #----- PRIVATE METHODS -----#
    def _query_table_list(self):
        """Retrieve a list of known Snowflake tables"""
        query = "SELECT concat(t.TABLE_SCHEMA,'.',t.TABLE_NAME) AS SFTABLE FROM\
        information_schema.tables as t WHERE t.table_schema != 'INFORMATION_SCHEMA'"
        tableList = self.sql(query).collect()
        
        _ = [self._sfTables.append(t.asDict().get('SF_TABLE')) for t in tableList]
        return None
