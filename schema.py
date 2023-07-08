from pyspark.sql.types import *
import pandas as pd
import pandera as pa
from pandera import DataFrameSchema
from marshmallow_pyspark import Schema
from marshmallow import fields


# DESIRED_SCHEMA = StructType([
#     StructField(name='order_id', dataType=IntegerType(), nullable=True),
#     StructField(name='order_date', dataType=TimestampType(), nullable=True),
#     StructField(name='order_customer_id', dataType=IntegerType(), nullable=True),
#     StructField(name='order_status', dataType=StringType(), nullable=True)
# ])


class SchemaCheck(Schema):
    order_id = fields.Int()
    order_date = fields.AwareDateTime()
    order_customer_id = fields.Int()
    order_status = fields.Str()

    # def __init__(self):
    #     self.retrieved_schema = self.generate_schema()
    #
    # # def val_schema(self, df):
    # #     if not df.schema == self.schema:
    # #         return False
    # #     return True
    #
    # @staticmethod
    # def generate_schema():
    #     """
    #     This is standalone right now, but once you have an API you can create the targetted schema based on
    #     what is pulled back from whatever data governance system them is. It would be handled in another method that
    #     would pull the API endpoint and store that into a class attribute then invoke this method
    #     """
    #     schema = DataFrameSchema(
    #         {
    #             "order_id": pa.Column(int),
    #             # "order_date": Column(pa.DateTime),
    #             "order_customer_id": pa.Column(int),
    #             "order_status": pa.Column(str)
    #         }
    #     )
    #     return schema
    #
    # def validate_schemas(self, df):
    #     self.retrieved_schema.validate(df)

