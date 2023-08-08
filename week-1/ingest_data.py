import argparse
import gzip
import os
from time import time

import pandas as pd
from sqlalchemy import create_engine


def upload(iterator, engine, table_name):
    while True:
        t_start = time()
        df = next(iterator)  # will raise exception if None left
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
        df.to_sql(con=engine, name=table_name, if_exists="append")
        t_end = time()
        print("Chunk took %.3f seconds" % (t_end - t_start))


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()

    file_name = url.split("/")[-1]
    os.system(f"wget {url} -O {file_name}")

    with gzip.open(file_name, "rb") as f:
        temp_file_name = "temp.csv"
        with open(temp_file_name, "wb") as temp_file:
            temp_file.write(f.read())

        df_iter = pd.read_csv(temp_file_name, iterator=True, chunksize=100_000)

        try:
            upload(df_iter, engine, table_name)
        except StopIteration:
            os.remove(temp_file_name)

    os.remove(file_name)
    print("Job finished successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")
    parser.add_argument("--user", help="username for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument("--table_name", help="name of the table")
    parser.add_argument("--url", help="url of the csv file")
    args = parser.parse_args()
    main(args)

# python ingest_data.py
#   --user="root"  \
#   --password="root"  \
#   --host="localhost"  \
#   --port="5432"  \
#   --db="ny_taxi"  \
#   --table_name="yellow_taxi_data"  \
#   --url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
