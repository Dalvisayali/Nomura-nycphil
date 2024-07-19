from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()

SNOWFLAKE_USER = 'dalvisayali97'
SNOWFLAKE_PASSWORD = 'Qwerty12345'
SNOWFLAKE_ACCOUNT = 'affjtns-bv95929'
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'NYCPHIL_DB'
SNOWFLAKE_SCHEMA = 'PUBLIC'

# Snowflake connection details
# SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
# SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
# SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
# SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
# SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
# SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

print(os.getenv("SNOWFLAKE_ACCOUNT"))

DATABASE_URL = (
    'snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'.format(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE
    )
)

engine = create_engine(DATABASE_URL, connect_args={'client_session_keep_alive': True})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

