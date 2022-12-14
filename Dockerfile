FROM python:3.10.6

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir \
    cerberus==1.3.4 \
    dataset==1.5.2 \
    numpy==1.23.5 \
    pandas==1.5.1 \
    psycopg2==2.9.3
