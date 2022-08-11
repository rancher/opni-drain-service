# Standard Library
import asyncio
import json
import logging
import math
import os
import sys
import time
from asyncio.exceptions import TimeoutError
from collections import deque

# Third Party
import numpy as np
import pandas as pd
import ruptures as rpt
from drain3.file_persistence import FilePersistence
from drain3.template_miner import TemplateMiner
from opni_nats import NatsWrapper
from opni_proto.log_anomaly_payload_pb import Payload, PayloadList

pd.set_option("mode.chained_assignment", None)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")

IS_PRETRAIN_SERVICE = os.getenv("IS_PRETRAINED_SERVICE", "true")

if "RETRAIN_OFTEN" in os.environ:
    RETRAIN_OFTEN = os.environ["RETRAIN_OFTEN"].lower() == "true"
else:
    RETRAIN_OFTEN = False

nw = NatsWrapper()

async def load_pretrain_model():
    # This function will load the pretrained DRAIN model for control plane logs in addition to the anomaly level for each template.
    try:
        pretrained_template_miner = TemplateMiner()
        pretrained_template_miner.load_state("drain3_pretrained_model_v0.6.1.bin")
        num_pretrained_clusters = pretrained_template_miner.drain.clusters_counter
        logging.info("Able to load the DRAIN control plane model with {} clusters.".format(num_pretrained_clusters))
        persistence = FilePersistence("drain3_non_workload_model.bin")
        current_template_miner = TemplateMiner(persistence_handler=persistence, clusters_counter=num_pretrained_clusters)
        return pretrained_template_miner, current_template_miner
    except Exception as e:
        logging.error(f"Unable to load DRAIN model {e}")
        return None, None

async def persist_model(batch_processed_queue, current_template_miner):
    last_upload = time.time()
    while True:
        processed_payload = await batch_processed_queue.get()
        current_time = time.time()
        if current_time - last_upload >= 3600:
            current_template_miner.save_state()
            last_upload = time.time()

async def update_model(update_model_logs_queue, current_template_miner):
    '''
    This function will process logs that were passed back by the Inferencing service. These log messages will be
    added to the pretrained DRAIN model in addition to the predicted anomaly level as well.
    '''
    while True:
        inferenced_logs = await update_model_logs_queue.get()
        final_inferenced_logs = []
        template_data = []
        for log_data in inferenced_logs.items:
            anomaly_level = log_data.anomaly_level
            result = current_template_miner.add_log_message(log_data.masked_log, log_data.log, anomaly_level)
            log_data.template_matched = result["template_mined"]
            log_data.template_cluster_id = result["cluster_id"]
            if result["change_type"] == "cluster_created" or result["change_type"] == "cluster_template_changed":
                template_data.append(Payload(log=result["sample_log"], template_matched=result["template_mined"], template_cluster_id=result["cluster_id"], _id=str(result["cluster_id"]), log_type=log_data.log_type))
            final_inferenced_logs.append(log_data)
        await nw.publish("inferenced_logs", bytes(PayloadList(items=final_inferenced_logs)))
        if len(template_data) > 0:
            await nw.publish("templates_index", bytes(PayloadList(items=template_data)))
        if IS_PRETRAIN_SERVICE:
            await nw.publish("batch_processed_workload", "processed".encode())
        else:
            await nw.publish("batch_processed", "processed".encode())

async def init_nats():
    # This function initialized the connection to Nats.
    logging.info("connecting to nats")
    await nw.connect()

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    incoming_cp_logs_queue = asyncio.Queue(loop=loop)
    update_model_logs_queue = asyncio.Queue(loop=loop)
    batch_processed_queue = asyncio.Queue(loop=loop)
    init_nats_task = loop.create_task(init_nats())
    loop.run_until_complete(init_nats_task)
    if IS_PRETRAIN_SERVICE:
        init_model_task = loop.create_task(load_pretrain_model())
        pretrained_template_miner, current_template_miner = loop.run_until_complete(init_model_task)
        if not pretrained_template_miner:
            sys.exit(1)

    preprocessed_logs_consumer_coroutine = consume_logs(incoming_cp_logs_queue, update_model_logs_queue, batch_processed_queue)

    match_cp_logs_coroutine = inference_logs(incoming_cp_logs_queue, pretrained_template_miner, current_template_miner)

    update_model_coroutine = update_model(update_model_logs_queue, current_template_miner)

    persist_model_coroutine = persist_model(batch_processed_queue, current_template_miner)

    loop.run_until_complete(
        asyncio.gather(
            preprocessed_logs_consumer_coroutine,
            match_cp_logs_coroutine,
            update_model_coroutine,
            persist_model_coroutine
        )
    )
    try:
        loop.run_forever()
    finally:
        loop.close()