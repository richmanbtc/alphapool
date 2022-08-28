FROM python:3.6.15

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir \
    cerberus==1.3.4 \
    dataset==1.5.2 \
    numpy==1.19.5 \
    pandas==1.1.5
