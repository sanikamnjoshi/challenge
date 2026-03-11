from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.types import *


def main():
    spark = SparkSession.builder \
        .appName("BankDataProcessor") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()

    print("SparkSession created.")

    try:
        bucket_name = "navique"
        s3_input_path = f"s3a://{bucket_name}/input"
        s3_output_path = f"s3a://{bucket_name}/output"

        # TODO sjo: If the input data were unreliable and not super clean, I could add schema definitions for strong typing instead of using inferSchema.
        #accounts_schema = StructType([
        #    StructField("account_id", IntegerType(), nullable=False),
        #    StructField("district_id", IntegerType(), nullable=False),
        #    StructField("frequency", StringType(), nullable=False),
        #    StructField("date", DateType(), nullable=False)
        #])
        # read source files
        accounts = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv(f"{s3_input_path}/account.csv")
        cards = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv(f"{s3_input_path}/card.csv")
        clients = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv(f"{s3_input_path}/client.csv")
        dispositions = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv(f"{s3_input_path}/disp.csv")
        districts = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv(f"{s3_input_path}/district.csv")
        loans = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv(f"{s3_input_path}/loan.csv")
        orders = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv(f"{s3_input_path}/order.csv")
        transactions = spark.read.option("header", True).option("sep", ";").option("inferSchema", True).csv(f"{s3_input_path}/trans.csv")

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
            .parquet(f"{s3_output_path}/cleaned-transactions.parquet.gzip")

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
            .csv(f"{s3_output_path}/aggregations/avg-loan-by-district.csv")

        print("Processing successful.")
        return {"status": "success"}

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {"status": "error", "message": str(e)}

    finally:
        spark.stop()
        print("SparkSession stopped.")


if __name__ == "__main__":
    main()