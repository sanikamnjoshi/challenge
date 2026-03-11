from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, avg
from pyspark.sql.types import *


def handler(event, context):
    spark = SparkSession.builder \
        .appName("process-finance-data") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    try:
        # TODO sjo: If the input data were unreliable and not super clean, I could add schema definitions for strong typing instead of using inferSchema.
        #accounts_schema = StructType([
        #    StructField("account_id", IntegerType(), nullable=False),
        #    StructField("district_id", IntegerType(), nullable=False),
        #    StructField("frequency", StringType(), nullable=False),
        #    StructField("date", DateType(), nullable=False)
        #])
        # read source files
        accounts = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv("input/account.csv")
        cards = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv("input/card.csv")
        clients = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv("input/client.csv")
        dispositions = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv("input/disp.csv")
        districts = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv("input/district.csv")
        loans = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv("input/loan.csv")
        orders = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv("input/order.csv")
        transactions = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv("input/trans.csv")

        # TODO sjo: irl, I could change the datatype of amount and balance to decimal
        #transactions = transactions \
        #    .withColumn("amount", col("amount").cast(DecimalType(14, 2))) \
        #    .withColumn("balance", col("balance").cast(DecimalType(14, 2)))


        # fix typo in transaction type
        transactions = transactions.replace(
            {"PRJIEM": "PRIJEM"},
            subset = ["type"]
        )
        #transactions.groupBy("type").count().orderBy(col("count").desc()).show(30, truncate=False)  # TODO sjo: sanity check

        # filter non-existent account ids from transactions, save to parquet
        valid_accounts = accounts.select("account_id").dropDuplicates()
        transaction_filtered = transactions.join(valid_accounts, on="account_id", how="inner")

        transaction_filtered.write \
            .mode("overwrite") \
            .option("compression", "gzip") \
            .parquet("output/cleaned-transactions.parquet.gzip")

        # calculate avg loan amt per district, save to csv
        loan_w_district = loans.select("loan_id", "account_id", "amount") \
            .join(accounts.select("account_id", "district_id"),
                  on="account_id",
                  how="left")
        #loan_w_district.show(30, truncate=False)  # TODO sjo: sanity check

        avg_loan_by_district = (
            loan_w_district
            .groupBy("district_id")
            .agg(avg("amount").alias("average_amount"))
            .orderBy("district_id")
        )

        avg_loan_by_district.write \
            .mode("overwrite") \
            .option("sep", ";") \
            .option("header", True) \
            .csv("output/aggregations/avg-loan-by-district.csv")

        print("Processing successful.")
        return {"status": "success"}

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {"status": "error", "message": str(e)}

    finally:
        spark.stop()
        print("Spark Session stopped.")
