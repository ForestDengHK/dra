# src/ara/pyspark/__init__.py
from dra.core.pyspark.session import SparkSessionManager
from dra.core.pyspark.exceptions import SparkException

try:
    # Initialize the session manager
    manager = SparkSessionManager()
    
    # Get common objects
    spark = manager.session
    sc = manager.sc
    sqlContext = manager.sql_context
    
    # Get utilities if available
    dbutils = manager.utils
    
    # Convenience methods
    table = spark.table
    sql = spark.sql
    
except SparkException as e:
    print(f"Spark Error: {str(e)}")
    raise
except Exception as e:
    print(f"Unexpected error: {str(e)}")
    raise