# Standard Library
import asyncio
import json
import logging
import math
import os
import time
from asyncio.exceptions import TimeoutError
from collections import deque

# Third Party
import numpy as np
import pandas as pd
import ruptures as rpt
from drain3.file_persistence import FilePersistence
from drain3.template_miner import TemplateMiner
from elasticsearch import AsyncElasticsearch, TransportError
from elasticsearch.exceptions import ConnectionTimeout
from elasticsearch.helpers import BulkIndexError, async_streaming_bulk
from opni_nats import NatsWrapper

pd.set_option("mode.chained_assignment", None)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
persistence = FilePersistence("drain3_state.bin")
template_miner = TemplateMiner(persistence)
ES_ENDPOINT = os.environ["ES_ENDPOINT"]
ES_USERNAME = os.environ["ES_USERNAME"]
ES_PASSWORD = os.environ["ES_PASSWORD"]
if "RETRAIN_OFTEN" in os.environ:
    RETRAIN_OFTEN = os.environ["RETRAIN_OFTEN"].lower() == "true"
else:
    RETRAIN_OFTEN = False

num_no_templates_change_tracking_queue = deque([], 50)
num_templates_changed_tracking_queue = deque([], 50)
num_clusters_created_tracking_queue = deque([], 50)
num_total_clusters_tracking_queue = deque([], 200)

nw = NatsWrapper()


async def consume_logs(incoming_logs_to_train_queue, logs_to_update_es):
    async def subscribe_handler(msg):
        payload_data = msg.data.decode()
        await incoming_logs_to_train_queue.put(
            pd.read_json(payload_data, dtype={"_id": object})
        )

    async def anomalies_subscription_handler(msg):
        anomalies_data = msg.data.decode()
        logging.info("got anomaly payload")
        await logs_to_update_es.put(pd.read_json(anomalies_data, dtype={"_id": object}))

    await nw.subscribe(
        nats_subject="preprocessed_logs",
        payload_queue=None,
        subscribe_handler=subscribe_handler,
    )

    await nw.subscribe(
        nats_subject="anomalies",
        payload_queue=None,
        subscribe_handler=anomalies_subscription_handler,
    )


async def train_and_inference(incoming_logs_to_train_queue, fail_keywords_str):
    global num_no_templates_change_tracking_queue, num_templates_changed_tracking_queue, num_clusters_created_tracking_queue
    while True:
        payload_data_df = await incoming_logs_to_train_queue.get()
        inferencing_results = []
        for index, row in payload_data_df.iterrows():
            log_message = row["masked_log"]
            if log_message:
                result = template_miner.add_log_message(log_message)
                d = {
                    "_id": row["_id"],
                    "update_type": result["change_type"],
                    "template_or_change_type": result
                    if result == "cluster_created"
                    else str(result["cluster_id"]),
                    "matched_template": result["template_mined"],
                    "drain_matched_template_id": result["cluster_id"],
                    "drain_matched_template_support": result["cluster_size"],
                }
                inferencing_results.append(d)

        if not inferencing_results:
            continue

        df = pd.DataFrame(inferencing_results)

        update_counts = df["update_type"].value_counts().to_dict()

        if "cluster_created" in update_counts:
            num_clusters_created_tracking_queue.appendleft(
                update_counts["cluster_created"]
            )

        if "cluster_template_changed" in update_counts:
            num_templates_changed_tracking_queue.appendleft(
                update_counts["cluster_template_changed"]
            )

        if "none" in update_counts:
            num_no_templates_change_tracking_queue.appendleft(update_counts["none"])

        # df['consecutive'] = df.template_or_change_type.groupby((df.template_or_change_type != df.template_or_change_type.shift()).cumsum()).transform('size')
        df["drain_prediction"] = 0
        if not fail_keywords_str:
            fail_keywords_str = "a^"
        df["drain_error_keyword"] = df["matched_template"].str.contains(fail_keywords_str, regex=True)
        df.loc[
            (df["drain_matched_template_support"] <= 10)
            & (df["drain_matched_template_support"] != 10)
            | (df["drain_error_keyword"] == True),
            "drain_prediction",
        ] = 1
        prediction_payload = (
            df[
                [
                    "_id",
                    "drain_prediction",
                    "drain_matched_template_id",
                    "drain_matched_template_support",
                    "drain_error_keyword",
                ]
            ]
                .to_json()
                .encode()
        )
        await nw.publish("anomalies", prediction_payload)


async def setup_es_connection():
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
    es = await setup_es_connection()

    async def doc_generator_anomaly(df):
        for index, document in df.iterrows():
            doc_dict = document.to_dict()
            yield doc_dict

    async def doc_generator(df):
        for index, document in df.iterrows():
            doc_dict = document.to_dict()
            doc_dict["doc"] = {}
            doc_dict["doc"]["drain_matched_template_id"] = doc_dict[
                "drain_matched_template_id"
            ]
            doc_dict["doc"]["drain_matched_template_support"] = doc_dict[
                "drain_matched_template_support"
            ]
            del doc_dict["drain_matched_template_id"]
            del doc_dict["drain_matched_template_support"]
            yield doc_dict

    while True:
        df = await queue.get()
        df["_op_type"] = "update"
        df["_index"] = "logs"

        # update anomaly_predicted_count and anomaly_level for anomalous logs
        anomaly_df = df[df["drain_prediction"] == 1]
        if len(anomaly_df) == 0:
            logging.info("No anomalies in this payload")
        else:
            script = (
                "ctx._source.anomaly_predicted_count += 1; ctx._source.drain_anomaly = true; ctx._source.anomaly_level = "
                "ctx._source.anomaly_predicted_count == 1 ? 'Suspicious' : ctx._source.anomaly_predicted_count == 2 ? "
                "'Anomaly' : 'Normal';"
            )
            anomaly_df["script"] = script
            try:
                async for ok, result in async_streaming_bulk(
                        es,
                        doc_generator_anomaly(
                            anomaly_df[["_id", "_op_type", "_index", "script"]]
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
                queue.put(anomaly_df)
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
                        df[
                            [
                                "_id",
                                "_op_type",
                                "_index",
                                "drain_matched_template_id",
                                "drain_matched_template_support",
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
            logging.info(f"Updated {len(df)} logs in ES")
        except (BulkIndexError, ConnectionTimeout) as exception:
            logging.error("Failed to index data")
            logging.error(exception)
            queue.put(df)
        except TransportError as exception:
            logging.info(f"Error in async_streaming_bulk {exception}")
            if exception.status_code == "N/A":
                logging.info("Elasticsearch connection error")
                es = await setup_es_connection()


async def training_signal_check():
    es = await setup_es_connection()

    def weighted_avg_and_std(values, weights):
        average = np.average(values, weights=weights)
        # Fast and numerically precise:
        variance = np.average((values - average) ** 2, weights=weights)
        return average, math.sqrt(variance)

    iteration = 0
    num_templates_in_last_train = 0
    num_prev_breakpoints = 0
    train_on_next_chance = True
    stable = False
    training_start_ts_ms = int(pd.to_datetime("now", utc=True).timestamp() * 1000)
    very_first_ts_ns = training_start_ts_ms

    normal_periods = []

    while True:
        await asyncio.sleep(20)
        num_drain_templates = len(template_miner.drain.clusters)
        if len(num_total_clusters_tracking_queue) == 0 and num_drain_templates == 0:
            logging.info("No DRAIN templates learned yet")
            continue
        iteration += 1
        num_total_clusters_tracking_queue.appendleft(num_drain_templates)
        logging.info(
            "training_signal_check: num_total_clusters_tracking_queue {}".format(
                num_total_clusters_tracking_queue
            )
        )

        if RETRAIN_OFTEN:
            # more aggressive retraining
            if len(list(num_total_clusters_tracking_queue)) > 10 and iteration % 180 == 0:
                signal = np.array(list(reversed(num_total_clusters_tracking_queue)))
                algo = rpt.Pelt(model="l1").fit(signal)
                my_bkps = algo.predict(pen=100)
                logging.info(f"breakpoints = {my_bkps}")
                if len(my_bkps) > 1 and len(my_bkps) != num_prev_breakpoints:
                    num_prev_breakpoints = len(my_bkps)
                    logging.info(f"num_prev_breakpoints = {num_prev_breakpoints}")
                    # calculate start_ts based on last change point
                    training_start_ts_ms = pd.to_datetime("now", utc=True).timestamp() * 1000 - my_bkps[-2] * 20000 + 20000
                    very_first_ts_ns = training_start_ts_ms
                    num_total_clusters_tracking_queue.rotate(my_bkps[-2]*-1)
                    train_on_next_chance = True
                    stable = False

        num_clusters = np.array(num_total_clusters_tracking_queue)[:50]
        vol = np.std(num_clusters) / np.mean(num_clusters[:10])
        time_steps = num_clusters.shape[0]
        weights = np.flip(np.true_divide(np.arange(1, time_steps + 1), time_steps))
        weighted_mean, weighted_std = weighted_avg_and_std(num_clusters, weights)
        weighted_vol = weighted_std / np.mean(num_clusters[:10])
        logging.info(
            "ITERATION {}: vol= {} weighted_vol= {} normal_periods={}".format(
                iteration, vol, weighted_vol, str(normal_periods)
            )
        )

        if len(num_total_clusters_tracking_queue) > 30:
            if weighted_vol >= 0.199:
                train_on_next_chance = True

            if (
                    weighted_vol < 0.199
                    and training_start_ts_ms != very_first_ts_ns
                    and train_on_next_chance
            ):
                training_start_ts_ms = int(
                    pd.to_datetime("now", utc=True).timestamp() * 1000
                )

            if weighted_vol > 0.155 and not train_on_next_chance and stable:
                training_end_ts_ms = int(
                    pd.to_datetime("now", utc=True).timestamp() * 1000
                )
                normal_periods.append(
                    {"start_ts": training_start_ts_ms, "end_ts": training_end_ts_ms}
                )
                stable = False
                training_start_ts_ms = -1.0

            if weighted_vol <= 0.15 and (
                    train_on_next_chance
                    or num_drain_templates > (2 * num_templates_in_last_train)
            ):
                num_templates_in_last_train = num_drain_templates
                logging.info(f"SENDING TRAIN SIGNAL on iteration {iteration}")
                if training_start_ts_ms != -1.0:
                    training_end_ts_ms = int(
                        pd.to_datetime("now", utc=True).timestamp() * 1000
                    )
                    normal_periods.append(
                        {"start_ts": training_start_ts_ms, "end_ts": training_end_ts_ms}
                    )
                train_payload = {
                    "start_ts": training_start_ts_ms,
                    "end_ts": training_end_ts_ms,
                }
                try:
                    await es.index(index="opni-normal-intervals", body=train_payload)
                except Exception as e:
                    logging.error("Error when indexing status to opni-normal-intervals")

                await nw.publish("train", json.dumps(train_payload).encode())
                train_on_next_chance = False
                stable = True

                training_start_ts_ms = int(
                    pd.to_datetime("now", utc=True).timestamp() * 1000
                )

                drain_status_doc = {
                    "num_log_clusters": num_drain_templates,
                    "update_type": "training_signal",
                    "timestamp": training_end_ts_ms,
                }
                try:
                    await es.index(
                        index="opni-drain-model-status", body=drain_status_doc
                    )
                except Exception as e:
                    logging.error(
                        "Error when indexing status to opni-drain-model-status"
                    )


async def init_nats():
    logging.info("connecting to nats")
    await nw.connect()


async def wait_for_index():
    es = await setup_es_connection()
    while True:
        try:
            exists = await es.indices.exists("opni-normal-intervals")
            if exists:
                break
            else:
                logging.info("waiting for opni-normal-intervals index")
                time.sleep(2)

        except TransportError as exception:
            logging.info(f"Error in es indices {exception}")
            if exception.status_code == "N/A":
                logging.info("Elasticsearch connection error")
                es = await setup_es_connection()


if __name__ == "__main__":
    fail_keywords_str = ""
    for fail_keyword in os.environ["FAIL_KEYWORDS"].split(","):
        if not fail_keyword:
            continue
        if len(fail_keywords_str) > 0:
            fail_keywords_str += f"|({fail_keyword})"
        else:
            fail_keywords_str += f"({fail_keyword})"
    logging.info(f"fail_keywords_str = {fail_keywords_str}")

    loop = asyncio.get_event_loop()
    incoming_logs_to_train_queue = asyncio.Queue(loop=loop)
    model_to_save_queue = asyncio.Queue(loop=loop)
    logs_to_update_in_elasticsearch = asyncio.Queue(loop=loop)

    # Run initialization tasks
    loop.run_until_complete(
        asyncio.gather(
            init_nats(),
            wait_for_index(),
        )
    )

    preprocessed_logs_consumer_coroutine = consume_logs(
        incoming_logs_to_train_queue, logs_to_update_in_elasticsearch
    )
    train_coroutine = train_and_inference(
        incoming_logs_to_train_queue, fail_keywords_str
    )
    update_es_coroutine = update_es_logs(logs_to_update_in_elasticsearch)
    training_signal_coroutine = training_signal_check()

    loop.run_until_complete(
        asyncio.gather(
            preprocessed_logs_consumer_coroutine,
            train_coroutine,
            update_es_coroutine,
            training_signal_coroutine,
        )
    )
    try:
        loop.run_forever()
    finally:
        loop.close()
