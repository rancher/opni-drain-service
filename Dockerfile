FROM rancher/opni-python-base:3.8

WORKDIR /app
COPY ./requirements.txt /app/
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY ./drain_service/ /app/
RUN chmod a+rwx -R /app

CMD ["python", "./drain_modules.py"]
