import json


def read_csv_to_df(spark, file_path):
    """
    Read CSV data to input Dataframe
    
    """
    return spark.read.option("inferSchema", "true").csv(file_path, header=True)


def read_json(file_path):
    """
    Read Config file in JSON format
    
    """
    with open(file_path, "r") as f:
        config_param=json.load(f)
        return config_param


def write_output_in_parquet(df, file_path):
    """
    Write dataframe to Parquet file
    
    """

    df.coalesce(1).write.format('parquet').mode('overwrite').option("header", "true").save(file_path)
    
