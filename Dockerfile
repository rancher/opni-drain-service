FROM rancher/opni-python-base:3.8
WORKDIR /code

COPY ./drain-service/drain_training_inferencing.py .
COPY ./drain-service/drain_pretrained_inferencing.py .
COPY ./drain-service/drain_modules.py .
COPY ./drain-service/payload_pb2.py .
COPY ./drain-service/drain3.ini .
COPY ./drain-service/drain3_control_plane_model_v0.4.1.bin .
ADD ./drain-service/drain3 /code/drain3
RUN pip install protobuf==3.19.4

CMD ["python", "./drain_modules.py"]
