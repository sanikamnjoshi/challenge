from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from schemas import SCHEMAS


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

        # read source files
        file_mapping = {
            "accounts": "account.csv",
            "cards": "card.csv",
            "clients": "client.csv",
            "dispositions": "disp.csv",
            "districts": "district.csv",
            "loans": "loan.csv",
            "orders": "order.csv",
            "transactions": "trans.csv"
        }

        dfs = {
            name: spark.read
            .option("header", True)
            .option("sep", ";")
            .schema(SCHEMAS[name])
            .csv(f"{s3_input_path}/{file_name}")
            for name, file_name in file_mapping.items()
        }

        transactions = dfs["transactions"]
        accounts = dfs["accounts"]
        loans = dfs["loans"]

        # fix typo in transaction type
        transactions = transactions.replace(
            {"PRJIEM": "PRIJEM"},
            subset = ["type"]
        )

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