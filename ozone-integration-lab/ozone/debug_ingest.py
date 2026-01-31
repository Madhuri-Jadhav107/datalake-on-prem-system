from pyspark.sql import SparkSession
import sys

print("--- DEBUG SCRIPT START ---")

# Completely empty builder - forcing command line dominance
spark = SparkSession.builder.getOrCreate()

print("--- Effective Configurations ---")
# Print all catalog related configs
for k, v in spark.sparkContext.getConf().getAll():
    if "catalog" in k or "fs." in k:
        print(f"{k} = {v}")

print("--- Attempting Local Write ---")
try:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
    
    # Create a tiny dataframe
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "data"])
    
    print("Writing to local.db.debug_table...")
    df.writeTo("local.db.debug_table").createOrReplace()
    
    print("Success! Table created.")
    spark.sql("SELECT * FROM local.db.debug_table").show()

except Exception as e:
    print("--- FAILURE ---")
    print(e)
    import traceback
    traceback.print_exc()

print("--- DEBUG SCRIPT END ---")
