FROM python:3.8-slim
WORKDIR /code

COPY ./drain-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./drain-service/drain_training_inferencing.py .
COPY ./drain-service/drain3.ini .
ADD ./drain-service/drain3 /code/drain3

CMD ["python", "./drain_training_inferencing.py"]
