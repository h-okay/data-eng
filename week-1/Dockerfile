FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]

# docker build -t taxi_ingest:v001 .
# docker run taxi_ingest:v001 
#    --user=root  \
#    --password=root  \
#    --host=host.docker.internal  \ --> using the dockerized local postgres
#    --port=5432  \
#    --db=ny_taxi  \
#    --table_name=yellow_taxi_data  \
#    --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz 
