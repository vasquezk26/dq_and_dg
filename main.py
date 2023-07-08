from spark_session import Session
from schema import SchemaCheck
import pandas as pd


if __name__ == "__main__":
    valid_schema = SchemaCheck()
    spark = Session('connections.json')
    my_df = spark.pull_data()
    # my_df.show()
    # print(my_df.schema)
    print(valid_schema.retrieved_schema)
    # print(valid_schema.val_schema(my_df))
    # print(valid_schema.generate_schema())
    valid_schema.validate_schemas(my_df)

    """WIll put this back once other work is tested."""
    # valid_df, error_df = valid_schema.validate_df(my_df)
    # valid_df.show()
    # error_df.show(truncate=False)
