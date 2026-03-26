FROM python:3.11-slim

WORKDIR /srv

COPY rest-server.py /srv/

RUN pip install --no-cache-dir \
    flask \
    redis \
    jsonpickle \
    requests \
    minio

EXPOSE 5000

CMD ["python", "rest-server.py"]