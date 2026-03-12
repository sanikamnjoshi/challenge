from pyspark.sql.types import *

SCHEMAS = {
    "accounts" : StructType([
        StructField("account_id", IntegerType(), nullable=False),
        StructField("district_id", IntegerType(), nullable=False),
        StructField("frequency", StringType(), nullable=False),
        StructField("date", DateType(), nullable=False)
    ]),

    "cards" : StructType([
        StructField("card_id", IntegerType(), nullable=False),
        StructField("disp_id", IntegerType(), nullable=False),
        StructField("type", StringType(), nullable=False),
        StructField("issued", DateType(), nullable=False)
    ]),

    "clients" : StructType([
        StructField("client_id", IntegerType(), nullable=False),
        StructField("gender", StringType(), nullable=False),
        StructField("birth_date", DateType(), nullable=False),
        StructField("district_id", IntegerType(), nullable=False)
    ]),

    "dispositions" : StructType([
        StructField("disp_id", IntegerType(), nullable=False),
        StructField("client_id", IntegerType(), nullable=False),
        StructField("account_id", IntegerType(), nullable=False),
        StructField("type", StringType(), nullable=False)
    ]),

    "districts" : StructType([
        StructField("district_id", IntegerType(), nullable=False),
        StructField("A2", StringType(), nullable=True),
        StructField("A3", StringType(), nullable=True),
        StructField("A4", IntegerType(), nullable=True),
        StructField("A5", IntegerType(), nullable=True),
        StructField("A6", IntegerType(), nullable=True),
        StructField("A7", IntegerType(), nullable=True),
        StructField("A8", IntegerType(), nullable=True),
        StructField("A9", IntegerType(), nullable=True),
        StructField("A10", DoubleType(), nullable=True),
        StructField("A11", DoubleType(), nullable=True),
        StructField("A12", DoubleType(), nullable=True),
        StructField("A13", DoubleType(), nullable=True),
        StructField("A14", IntegerType(), nullable=True),
        StructField("A15", IntegerType(), nullable=True),
        StructField("A16", IntegerType(), nullable=True)
    ]),

    "loans" : StructType([
        StructField("loan_id", IntegerType(), nullable=False),
        StructField("account_id", IntegerType(), nullable=False),
        StructField("date", DateType(), nullable=False),
        StructField("amount", DoubleType(), nullable=False),
        StructField("duration", IntegerType(), nullable=False),
        StructField("payments", DoubleType(), nullable=False),
        StructField("status", StringType(), nullable=False)
    ]),

    "orders" : StructType([
        StructField("order_id", IntegerType(), nullable=False),
        StructField("account_id", IntegerType(), nullable=False),
        StructField("bank_to", StringType(), nullable=False),
        StructField("account_to", IntegerType(), nullable=False),
        StructField("amount", DoubleType(), nullable=False),
        StructField("k_symbol", StringType(), nullable=True)
    ]),

    "transactions" : StructType([
        StructField("trans_id", IntegerType(), nullable=False),
        StructField("account_id", IntegerType(), nullable=False),
        StructField("date", DateType(), nullable=False),
        StructField("type", StringType(), nullable=False),
        StructField("operation", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=False),
        StructField("balance", DoubleType(), nullable=False),
        StructField("k_symbol", StringType(), nullable=True),
        StructField("branch", StringType(), nullable=True),
        StructField("bank", StringType(), nullable=True),
        StructField("account", IntegerType(), nullable=True)
    ]),
}
