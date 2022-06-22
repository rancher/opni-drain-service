FROM rancher/opni-python-base:3.8

COPY ./drain-service/ /app/
RUN pip install protobuf==3.19.4
RUN chmod a+rwx -R /app
WORKDIR /app

CMD ["python", "./drain_modules.py"]
