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
from opni_nats import NatsWrapper
from opni_proto.log_anomaly_payload_pb import Payload, PayloadList

pd.set_option("mode.chained_assignment", None)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")
if "RETRAIN_OFTEN" in os.environ:
    RETRAIN_OFTEN = os.environ["RETRAIN_OFTEN"].lower() == "true"
else:
    RETRAIN_OFTEN = False

num_no_templates_change_tracking_queue = deque([], 50)
num_templates_changed_tracking_queue = deque([], 50)
num_clusters_created_tracking_queue = deque([], 50)
num_total_clusters_tracking_queue = deque([], 200)
resources_to_track = []

nw = NatsWrapper()

def match_template(log_data, template_miner):
    result = template_miner.match(log_data.masked_log, log_data.log)
    template, anomaly_level, cluster_id, template_log = result["template"], result["anomaly_level"], result["template_cluster_id"], result["template_log"]
    if template:
        log_data.anomaly_level = anomaly_level
        log_data.template_cluster_id = cluster_id
        log_data.template_matched = template
        log_data.inference_model = "drain"
        if template_log:
            template_log_payload = Payload(log=template_log, template_matched=template, template_cluster_id=cluster_id, _id=str(cluster_id))
            return True, template_log_payload
        return True, None
    return False, None


async def consume_logs(incoming_logs_to_train_queue, update_model_logs_queue, batch_processed_queue):
    async def subscribe_handler(msg):
        payload_data = msg.data
        logs_payload_list = PayloadList()
        logs_payload = logs_payload_list.parse(payload_data)
        await incoming_logs_to_train_queue.put(
            logs_payload
        )
    async def inferenced_subscribe_handler(msg):
        payload_data = msg.data
        logs_payload_list = PayloadList()
        logs_payload = logs_payload_list.parse(payload_data)
        await update_model_logs_queue.put(logs_payload)

    await nw.subscribe(
        nats_subject="batch_processed_workload",
        nats_queue="workers",
        payload_queue=batch_processed_queue
    )

    await nw.subscribe(
        nats_subject="model_inferenced_workload_logs",
        nats_queue="workers",
        payload_queue=update_model_logs_queue,
        subscribe_handler=inferenced_subscribe_handler,
    )

    await nw.subscribe(
        nats_subject="preprocessed_logs_workload",
        payload_queue=None,
        subscribe_handler=subscribe_handler,
    )

async def persist_model(batch_processed_queue, current_template_miner):
    last_upload = time.time()
    while True:
        processed_payload = await batch_processed_queue.get()
        current_time = time.time()
        if current_time - last_upload >= 3600:
            current_template_miner.save_state()
            last_upload = time.time()

async def inference_logs(incoming_logs_queue, workload_template_miner):
    '''
        This function will be inferencing on logs which are sent over through Nats and using the DRAIN model to match the logs to a template.
        If no match is made, the log is then sent over to be inferenced on by the Deep Learning model.
    '''
    last_time = time.time()
    logs_inferenced_results = []
    log_templates_modified = []
    workload_model_logs = []
    while True:
        logs_payload = await incoming_logs_queue.get()
        start_time = time.time()
        logging.info("Received payload of size {}".format(len(logs_payload.items)))
        for log_data in logs_payload.items:
            log_message = log_data.masked_log
            if log_message:
                current_template_matched, current_template_payload = match_template(log_data, workload_template_miner)
                if current_template_matched:
                    logs_inferenced_results.append(log_data)
                    logging.info(log_data.template_matched)
                    if current_template_payload:
                        log_templates_modified.append(current_template_payload)
                else:
                    workload_model_logs.append(log_data)
        logging.info(len(workload_model_logs))
        logging.info(logs_inferenced_results)
        if (start_time - last_time >= 1) or len(logs_inferenced_results) >= 128 or len(log_templates_modified) >= 128:
            if len(logs_inferenced_results) > 0:
                await nw.publish("inferenced_logs", bytes(PayloadList(items=logs_inferenced_results)))
                logs_inferenced_results = []
            if len(log_templates_modified) > 0:
                await nw.publish("templates_index", bytes(PayloadList(items=log_templates_modified)))
                log_templates_modified = []
            last_time = start_time
        if len(workload_model_logs) > 0:
            await nw.publish("opnilog_workload_logs", bytes(PayloadList(items = workload_model_logs)))
            logging.info(f"Published {len(workload_model_logs)} logs to be inferenced on by Workload Deep Learning model.")
            workload_model_logs = []

        logging.info(f"{len(logs_payload.items)} logs processed in {(time.time() - start_time)} second(s)")

async def update_model(update_model_logs_queue, workload_template_miner):
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
            result = workload_template_miner.add_log_message(log_data.masked_log, log_data.log, anomaly_level)
            log_data.template_matched = result["template_mined"]
            log_data.template_cluster_id = result["cluster_id"]
            if result["change_type"] == "cluster_created" or result["change_type"] == "cluster_template_changed":
                template_data.append(Payload(log=result["sample_log"], template_matched=result["template_mined"], template_cluster_id=result["cluster_id"], _id="0{}".format(result["cluster_id"]), log_type=log_data.log_type))
            final_inferenced_logs.append(log_data)
        await nw.publish("inferenced_logs", bytes(PayloadList(items = final_inferenced_logs)))
        await nw.publish("batch_processed_workload", "processed".encode())
        if len(template_data) > 0:
            await nw.publish("templates_index", bytes(PayloadList(items=template_data)))

'''
async def train_and_inference(incoming_logs_to_train_queue, fail_keywords_str):
    global num_no_templates_change_tracking_queue, num_templates_changed_tracking_queue, num_clusters_created_tracking_queue
    while True:
        logs_payload = await incoming_logs_to_train_queue.get()
        inferencing_results = []
        for log_data in logs_payload.iterrows():
            log_message = log_data.masked_log
            if log_message:
                result = template_miner.add_log_message(log_message)
                d = {
                    "_id": log_data.["_id"],
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
'''


'''
async def training_signal_check(workload_template_miner):

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
        num_drain_templates = len(workload_template_miner.drain.clusters)
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
'''

async def init_nats():
    logging.info("connecting to nats")
    await nw.connect()

def main():
    fail_keywords_str = ""
    for fail_keyword in os.environ["FAIL_KEYWORDS"].split(","):
        if not fail_keyword:
            continue
        if len(fail_keywords_str) > 0:
            fail_keywords_str += f"|({fail_keyword})"
        else:
            fail_keywords_str += f"({fail_keyword})"
    logging.info(f"fail_keywords_str = {fail_keywords_str}")
    persistence = FilePersistence("workload_drain_model.bin")
    workload_template_miner = TemplateMiner(persistence)
    loop = asyncio.get_event_loop()
    incoming_logs_queue = asyncio.Queue(loop=loop)
    update_model_queue = asyncio.Queue(loop=loop)
    batch_processed_queue = asyncio.Queue(loop=loop)


    # Run initialization tasks
    loop.run_until_complete(
        asyncio.gather(
            init_nats(),
        )
    )

    preprocessed_logs_consumer_coroutine = consume_logs(incoming_logs_queue, update_model_queue, batch_processed_queue)
    inference_coroutine = inference_logs(incoming_logs_queue, workload_template_miner)
    update_model_coroutine = update_model(update_model_queue, workload_template_miner)
    persist_model_coroutine = persist_model(batch_processed_queue, workload_template_miner)
    #training_signal_coroutine = training_signal_check(workload_template_miner)

    loop.run_until_complete(
        asyncio.gather(
            preprocessed_logs_consumer_coroutine,inference_coroutine, update_model_coroutine, persist_model_coroutine,
        )
    )
    try:
        loop.run_forever()
    finally:
        loop.close()