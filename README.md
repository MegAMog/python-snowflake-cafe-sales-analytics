# ☕ Python Snowflake Café Sales Analytics
This project demonstrates a complete **ETL (Extract, Transform, Load)** pipeline built in Python to process cafe sales data and load it into **Snowflake** for analytics and reporting.

## Step 1. Snowflake 
* Create a `.env` file in the project root and add your key. Ensure Snowflake credentials in .env are correct before running the ETL pipeline.
```env
SNOWFLAKE_ACCOUNT='your_account_here'
SNOWFLAKE_USER='your_user_here'
SNOWFLAKE_PASSWORD='your_password_here'
SNOWFLAKE_WAREHOUSE='your_warehouse_here'
SNOWFLAKE_DATABASE='your_database_here'
SNOWFLAKE_SCHEMA='your_schema_here'
SNOWFLAKE_ROLE='your_role_here'
```

## Step 2. Set Up the Environment
1. Create virtual environment (with python or python3 depending on your Python version)
```bash
python3 -m venv pythonvenv
```
2. Activate it
- macOS/Linux (bash):
``` bash
source venv/bin/activate
```
- Windows (PowerShell):
``` powershell
.\venv\Scripts\activate
```
3. Install dependencies
```bash
pip install -r requirements.txt
```

## Step 3. Run the App
1. cd to to src/
2. Run the main script
Execute script.py using python or python3:
```bash
python3 main.py
```


## Project Structure
```bash
├── .gitignore
├── data
│   └── raw_data.txt
├── LICENSE
├── README.md
├── requirements.txt
└── src
    ├── etl_pipeline.py
    ├── main.py
    └── utils.py
```

## Notes
- Make sure your `.env` file is not committed to Git (add it to .gitignore).
- After installing new packages, update dependencies with:
```bash
pip freeze > requirements.txt
```