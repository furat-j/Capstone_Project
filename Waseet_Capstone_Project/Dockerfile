# The stock Airflow image carries neither pandas nor a Postgres driver.
# Building once is cheaper than installing on every container start.
FROM apache/airflow:2.10.5

# pandas 2.1.4, not 2.2: Airflow 2.10 needs SQLAlchemy 1.4, pandas 2.2 needs
# SQLAlchemy 2.0, and the mismatch breaks `to_sql` with
# `'Connection' object has no attribute 'cursor'`. See Lecture 14's Dockerfile.
RUN pip install --no-cache-dir \
      pandas==2.1.4 \
      SQLAlchemy==1.4.54 \
      psycopg2-binary==2.9.9 \
      requests==2.32.3
