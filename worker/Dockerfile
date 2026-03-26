FROM xserrat/facebook-demucs:latest

WORKDIR /app

COPY worker-server.py /app/worker-server.py

RUN pip3 install redis minio requests

ENTRYPOINT ["python3", "/app/worker-server.py"]