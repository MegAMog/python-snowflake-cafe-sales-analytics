import pandas as pd
import os
import snowflake.connector
import snowflake.connector.pandas_tools as snowflake_tools

from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

# Extract data from a text file into a pandas DataFrame
def extract_data(txt_file)->pd.DataFrame:
    try:
        raw_data_df = pd.read_csv(txt_file, sep=r'\s+', header=None)
        
        # Assign column names
        raw_data_df.columns = ['client_name', 'item_name', 'price', 'branch_location', 'payment_method', 'payment_type_id', 'transaction_date']

        return raw_data_df
    except Exception as e:
        print(f"Error reading {txt_file}: {e}")
        return None
    

# Transform the DataFrame by cleaning and structuring the data
def transform_data(raw_data_df:pd.DataFrame)->pd.DataFrame:
    if raw_data_df is None:
        return None
    
    else:
        print("Transforming data...")
        # Step 1: Remove duplicates
        transformed_data_df = raw_data_df.copy()
        transformed_data_df = raw_data_df.drop_duplicates()

        # Step 2: # Remove PII date (client name) - drop columns by column name
        transformed_data_df = transformed_data_df.drop(columns=['client_name'], axis=1)

        #Step 3: Convert 'date' column to datetime format
        transformed_data_df['transaction_date'] = pd.to_datetime(transformed_data_df['transaction_date'], dayfirst=True, errors='coerce').dt.strftime("%Y-%m-%d")


        #Step 4: Convert 'price' column to numeric, coerce errors to NaN
        transformed_data_df['price'] = pd.to_numeric(transformed_data_df['price'].str[1:], errors='coerce').astype(float)


        # Step 5: Handle missing values - drop rows with any null values
        transformed_data_df.dropna(inplace=True)


        return transformed_data_df
    
# Load the transformed DataFrame to table in Snowflake
def load_data_to_snowflake(transformed_data_df:pd.DataFrame, table_name:str):
    if transformed_data_df is None:
        print("No data to load.")
        return None
    
    else:
        try:
            # Snowflake connection parameters from .env
            SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
            SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
            SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
            SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
            SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
            SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
            SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

            # Establish Snowflake connection
            conn = snowflake.connector.connect(
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                account=SNOWFLAKE_ACCOUNT,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA,
                role=SNOWFLAKE_ROLE
            )

            cursor = conn.cursor()

            # Define the table schema
            dftype_map = {
                'item_name': 'VARCHAR',
                'price': 'FLOAT',
                'branch_location': 'VARCHAR',
                'payment_method': 'VARCHAR',
                'payment_type_id': 'VARCHAR',
                'transaction_date': 'DATE'
            }

            columns_sql = ", ".join([f"{col.upper()} {dtype}" for col, dtype in dftype_map.items()])

            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name.upper()} (
                {columns_sql}
            );
            """
            cursor.execute(create_table_query)

            transformed_data_df.columns = [column.upper() for column in transformed_data_df.columns]
            transformed_data_df = transformed_data_df.reset_index(drop=True)
            
            success, nchunks, nrows, _ = snowflake_tools.write_pandas(conn=conn, df=transformed_data_df, table_name=table_name.upper())

            if success:
                print(f"Successfully loaded {nrows} rows into {table_name} table.")
                return nrows
            else:
                print("Failed to load data into Snowflake.")
                return None

        except Exception as e:
            print(f"Error loading data: {e}")
            return None
        
        finally:
            try:
                cursor.close()
                conn.close()
            except:
                pass
 




