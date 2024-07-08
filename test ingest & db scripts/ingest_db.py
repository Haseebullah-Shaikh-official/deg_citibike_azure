import os
from dotenv import load_dotenv
import pandas as pd
from glob import glob
from datetime import timedelta
import sqlalchemy as sa
from db.db_fun import create_table, insert_records

# Load environment variables from .env file
load_dotenv()

# Get database connection parameters from environment variables
# pg_host = os.getenv("PGHOST")
# pg_user = os.getenv("PGUSER")
# pg_port = os.getenv("PGPORT")
# pg_database = os.getenv("PGDATABASE")
# pg_password = os.getenv("PGPASSWORD")


# engine = sa.create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}')
# print(engine)

trips_csv = glob("./db_ingest/01_CITIBIKE_ZERO_TO_SNOWFLAKE_02.csv")


trips_df = pd.concat((pd.read_csv(file) for file in trips_csv), ignore_index=True)
# print(trips_df.info())
# print(trips_df.count())

trips_df['STARTTIME'] = pd.to_datetime(trips_df['STARTTIME'])
trips_df['STOPTIME'] = pd.to_datetime(trips_df['STOPTIME'])

trips_df["PRE_REGISTRATION_STATUS"] = True
trips_df["REGISTRATION_DATE"] = trips_df["STARTTIME"].dt.date - timedelta(days=2)

# print(trips_df.info())

# trips_df.to_sql('registered_trips', engine, if_exists="replace")






create_table()
insert_records("registered_trips", trips_df)
