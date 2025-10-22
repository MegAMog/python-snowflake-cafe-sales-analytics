import etl_pipeline as etl
import os
from utils import data_dir


if __name__ == "__main__":
    txt_file = os.path.join(data_dir, "raw_data.txt")
    raw_data_df = etl.extract_data(txt_file)

    transformed_data_df = etl.transform_data(raw_data_df)
    
    result = etl.load_data_to_snowflake(transformed_data_df, table_name="transactions")

    
    

