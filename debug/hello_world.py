from pyspark.sql import SparkSession
from delta import *

def main():
    """Main function to demonstrate basic Spark and Delta functionality."""
    # Create Spark session
    spark = SparkSession.getActiveSession()
    
    # Print hello world
    print("Hello from DRA!")

    
    keys = spark.conf.getAll
    print(any('databricks' in key.lower() for key in keys))
    # Print Spark version
    print(f"\nSpark Version: {spark.version}")

    # Stop Spark session
    # spark.stop()

    # Print Spark version
    # # Create a simple DataFrame
    # data = [("Hello", 1), ("World", 2)]
    # df = spark.createDataFrame(data, ["message", "count"])
    
    # # Show the DataFrame
    # print("\nCreated DataFrame:")
    # df.show()
    
    # # Write as Delta format
    # print("\nWriting to Delta format...")
    # df.write.format("delta").mode("overwrite").save("/tmp/dra_hello_world")
    
    # # Read back the Delta table
    # print("\nReading from Delta format:")
    # df_read = spark.read.format("delta").load("/tmp/dra_hello_world")
    # df_read.show()
    
    # # Print Spark version
    # print(f"\nSpark Version: {spark.version}")
    
    # # Stop Spark session
    # spark.stop()

if __name__ == "__main__":
    main() 