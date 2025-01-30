from pyspark.sql import SparkSession
from delta import *

def create_spark_session() -> SparkSession:
    """Create a Spark session with Delta Lake support."""
    return (SparkSession.builder
            .appName("DRA Hello World")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())

def main():
    """Main function to demonstrate basic Spark and Delta functionality."""
    # Create Spark session
    spark = create_spark_session()
    
    # Print hello world
    print("Hello from DRA!")
    
    # Create a simple DataFrame
    data = [("Hello", 1), ("World", 2)]
    df = spark.createDataFrame(data, ["message", "count"])
    
    # Show the DataFrame
    print("\nCreated DataFrame:")
    df.show()
    
    # Write as Delta format
    print("\nWriting to Delta format...")
    df.write.format("delta").mode("overwrite").save("/tmp/dra_hello_world")
    
    # Read back the Delta table
    print("\nReading from Delta format:")
    df_read = spark.read.format("delta").load("/tmp/dra_hello_world")
    df_read.show()
    
    # Print Spark version
    print(f"\nSpark Version: {spark.version}")
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main() 