# Standard Library
import asyncio
import json
import logging
import os
import urllib.request
from asyncio.exceptions import TimeoutError
import time

# Third Party
import pandas as pd
from drain3.file_persistence import FilePersistence
from drain3.template_miner import TemplateMiner
from elasticsearch import AsyncElasticsearch, TransportError
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch.helpers import BulkIndexError, async_streaming_bulk
from opni_nats import NatsWrapper

pd.set_option("mode.chained_assignment", None)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
persistence = FilePersistence("drain3_state.bin")
workload_template_miner = TemplateMiner(persistence)
cp_template_miner = TemplateMiner()
ES_ENDPOINT = os.environ["ES_ENDPOINT"]
ES_USERNAME = os.environ["ES_USERNAME"]
ES_PASSWORD = os.environ["ES_PASSWORD"]

nw = NatsWrapper()

def load_pretrained_model_anomaly_levels():
    # This function will load the anomaly levels of the templates from the pretrained DRAIN model.
    cp_predictions = dict()
    drain_preds_url = "https://opni-public.s3.us-east-2.amazonaws.com/pretrain-drain-cp-models/drain3_control_plane_preds.json"
    try:
        urllib.request.urlretrieve(drain_preds_url, "drain3_control_plane_preds.json")
        with open("drain3_control_plane_preds.json","r") as cp_preds:
            cp_predictions = json.load(cp_preds)
        logging.info("Able to load the control plane predictions for the {} clusters".format(len(cp_predictions)))
        return cp_predictions
    except Exception as e:
        logging.error(e)
        return cp_predictions

async def load_pretrain_model():
    # This function will load the pretrained DRAIN model for control plane logs in addition to the anomaly level for each template.
    drain_model_url = "https://opni-public.s3.us-east-2.amazonaws.com/pretrain-drain-cp-models/drain3_control_plane_model.bin"
    try:
        urllib.request.urlretrieve(drain_model_url, "drain3_control_plane_model.bin")
        logging.info("Successfully able to retrieve the control plane model and its corresponding predictions.")
        cp_template_miner.load_state("drain3_control_plane_model.bin")
        logging.info("Able to load the DRAIN control plane model with {} clusters.".format(cp_template_miner.drain.clusters_counter))
        return True
    except Exception as e:
        logging.error(f"Unable to load DRAIN model {e}")
        return False

async def consume_logs(incoming_cp_logs_queue, logs_to_update_es_cp):
    # This function will subscribe to the Nats subjects preprocessed_logs_control_plane and anomalies.
    async def subscribe_handler(msg):
        payload_data = msg.data.decode()
        await incoming_cp_logs_queue.put(
            pd.read_json(payload_data, dtype={"_id": object})
        )

    async def anomalies_subscription_handler(msg):
        anomalies_data = msg.data.decode()
        await logs_to_update_es_cp.put(pd.read_json(anomalies_data, dtype={"_id": object}))

    await nw.subscribe(
        nats_subject="preprocessed_logs_control_plane",
        nats_queue="workers",
        payload_queue=incoming_cp_logs_queue,
        subscribe_handler=subscribe_handler,
    )

    await nw.subscribe(
        nats_subject="anomalies",
        nats_queue="workers",
        payload_queue=logs_to_update_es_cp,
        subscribe_handler=anomalies_subscription_handler,
    )

async def inference_cp_logs(incoming_cp_logs_queue):
    '''
        This function will be inferencing on logs which are sent over through Nats and using the DRAIN model to match the logs to a template.
        If no match is made, the log is then sent over to be inferenced on by the Nulog Deep Learning model.
    '''
    cp_predictions = load_pretrained_model_anomaly_levels()
    while True:
        cp_logs_df = await incoming_cp_logs_queue.get()
        start_time = time.time()
        logging.info("Received payload of size {}".format(len(cp_logs_df)))
        logs_inferenced_results = []
        nulog_logs = []
        for index, row in cp_logs_df.iterrows():
            log_message = row["masked_log"]
            if log_message:
                row_dict = row.to_dict()
                result = cp_template_miner.match(log_message)
                if result:
                    result_template = result.get_template()
                    prediction = cp_predictions[result_template]
                    row_dict["anomaly_level"] = prediction
                    row_dict["drain_control_plane_template_matched"] = result_template
                    logs_inferenced_results.append(row_dict)
                else:
                    nulog_logs.append(row_dict)
        if len(logs_inferenced_results) > 0:
            logs_inferenced_drain_df = (pd.DataFrame(logs_inferenced_results).to_json().encode())
            await nw.publish("anomalies", logs_inferenced_drain_df)
        if len(nulog_logs) > 0:
            nulog_logs_df = pd.DataFrame(nulog_logs).to_json().encode()
            await nw.publish("nulog_cp_logs", nulog_logs_df)
            logging.info(f"Published {len(nulog_logs)} logs to be inferenced on by Nulog.")
        logging.info(f"{len(cp_logs_df)} logs processed in {(time.time() - start_time)} second(s)")


async def setup_es_connection():
    # This function will be setting up the Opensearch connection.
    logging.info("Setting up AsyncElasticsearch")
    return AsyncElasticsearch(
        [ES_ENDPOINT],
        port=9200,
        http_auth=(ES_USERNAME, ES_PASSWORD),
        http_compress=True,
        max_retries=10,
        retry_on_status={100, 400, 503},
        retry_on_timeout=True,
        timeout=20,
        use_ssl=True,
        verify_certs=False,
        sniff_on_start=False,
        # refresh nodes after a node fails to respond
        sniff_on_connection_fail=True,
        # and also every 60 seconds
        sniffer_timeout=60,
        sniff_timeout=10,
    )

async def update_es_logs(queue):
    # This function will be updating Opensearch logs which were inferred on by the DRAIN model.
    es = await setup_es_connection()

    async def doc_generator(df):
        for index, document in df.iterrows():
            doc_dict = document.to_dict()
            doc_dict["doc"] = {}
            doc_dict["doc"]["drain_control_plane_template_matched"] = doc_dict["drain_control_plane_template_matched"]
            if "anomaly_level" in doc_dict:
                doc_dict["doc"]["anomaly_level"] = doc_dict["anomaly_level"]
                del doc_dict["anomaly_level"]
            del doc_dict["drain_control_plane_template_matched"]
            yield doc_dict

    while True:
        df = await queue.get()
        df["_op_type"] = "update"
        df["_index"] = "logs"
        normal_df = df[df["anomaly_level"] == "Normal"]
        anomaly_df = df[df["anomaly_level"] == "Anomaly"]
        if len(anomaly_df) == 0:
            logging.info("No anomalies in this payload")
        else:
            try:
                async for ok, result in async_streaming_bulk(
                        es,
                        doc_generator(
                            anomaly_df[["_id", "_op_type", "_index", "drain_control_plane_template_matched", "anomaly_level"]]
                        ),
                        max_retries=1,
                        initial_backoff=1,
                        request_timeout=5,
                ):
                    action, result = result.popitem()
                    if not ok:
                        logging.error("failed to {} document {}".format())
                logging.info(f"Updated {len(anomaly_df)} anomalies in ES")
            except (BulkIndexError, ConnectionTimeout, TimeoutError) as exception:
                logging.error(
                    "Failed to index data. Re-adding to logs_to_update_in_elasticsearch queue"
                )
                logging.error(exception)
                await queue.put(anomaly_df)
            except TransportError as exception:
                logging.info(f"Error in async_streaming_bulk {exception}")
                if exception.status_code == "N/A":
                    logging.info("Elasticsearch connection error")
                    es = await setup_es_connection()

        try:
            # update normal logs in ES
            async for ok, result in async_streaming_bulk(
                    es,
                    doc_generator(
                        normal_df[
                            [
                                "_id",
                                "_op_type",
                                "_index",
                                "drain_control_plane_template_matched"
                            ]
                        ]
                    ),
                    max_retries=1,
                    initial_backoff=1,
                    request_timeout=5,
            ):
                action, result = result.popitem()
                if not ok:
                    logging.error("failed to {} document {}".format())
            logging.info(f"Updated {len(normal_df)} normal logs in ES")
        except (BulkIndexError, ConnectionTimeout) as exception:
            logging.error("Failed to index data")
            logging.error(exception)
            await queue.put(df)
        except TransportError as exception:
            logging.info(f"Error in async_streaming_bulk {exception}")
            if exception.status_code == "N/A":
                logging.info("Elasticsearch connection error")
                es = await setup_es_connection()

async def init_nats():
    # This function initialized the connection to Nats.
    logging.info("connecting to nats")
    await nw.connect()


async def wait_for_index():
    # This function is used to setup the Opensearch connection.
    es = await setup_es_connection()

def main():
    loop = asyncio.get_event_loop()
    incoming_cp_logs_queue = asyncio.Queue(loop=loop)
    cp_logs_to_update_in_elasticsearch = asyncio.Queue(loop=loop)

    # Run initialization tasks
    loop.run_until_complete(
        asyncio.gather(
            init_nats(),
            wait_for_index(),
        )
    )

    init_model_task = loop.create_task(load_pretrain_model())
    loop.run_until_complete(init_model_task)

    preprocessed_logs_consumer_coroutine = consume_logs(
        incoming_cp_logs_queue, cp_logs_to_update_in_elasticsearch
    )

    match_cp_logs_coroutine = inference_cp_logs(incoming_cp_logs_queue)

    update_es_cp_coroutine = update_es_logs(cp_logs_to_update_in_elasticsearch)

    loop.run_until_complete(
        asyncio.gather(
            preprocessed_logs_consumer_coroutine,
            match_cp_logs_coroutine,
            update_es_cp_coroutine,
        )
    )
    try:
        loop.run_forever()
    finally:
        loop.close()