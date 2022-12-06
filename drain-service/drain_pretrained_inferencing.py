# Standard Library
import asyncio
import logging
import sys
import time

# Third Party
from opni_proto.log_anomaly_payload_pb import Payload, PayloadList
from drain3.file_persistence import FilePersistence
from drain3.template_miner import TemplateMiner
from opni_nats import NatsWrapper

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(message)s")

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

async def consume_logs(incoming_cp_logs_queue, update_model_logs_queue, batch_processed_queue):
    # This function will subscribe to the Nats subjects preprocessed_logs_control_plane and anomalies.
    async def subscribe_handler(msg):
        payload_data = msg.data
        logs_payload_list = PayloadList()
        logs_payload = logs_payload_list.parse(payload_data)
        await incoming_cp_logs_queue.put(logs_payload)

    async def inferenced_subscribe_handler(msg):
        payload_data = msg.data
        logs_payload_list = PayloadList()
        logs_payload = logs_payload_list.parse(payload_data)
        await update_model_logs_queue.put(logs_payload)

    await nw.subscribe(
        nats_subject="preprocessed_logs_pretrained_model",
        nats_queue="workers",
        payload_queue=incoming_cp_logs_queue,
        subscribe_handler=subscribe_handler,
    )

    await nw.subscribe(
        nats_subject="batch_processed",
        nats_queue="workers",
        payload_queue=batch_processed_queue
    )

    await nw.subscribe(
        nats_subject="model_inferenced_pretrained_logs",
        nats_queue="workers",
        payload_queue=update_model_logs_queue,
        subscribe_handler=inferenced_subscribe_handler,
    )

async def persist_model(batch_processed_queue, current_template_miner):
    last_upload = time.time()
    while True:
        processed_payload = await batch_processed_queue.get()
        current_time = time.time()
        if current_time - last_upload >= 3600:
            current_template_miner.save_state()
            last_upload = time.time()


async def inference_logs(incoming_logs_queue, pretrained_template_miner, current_template_miner):
    '''
        This function will be inferencing on logs which are sent over through Nats and using the DRAIN model to match the logs to a template.
        If no match is made, the log is then sent over to be inferenced on by the Deep Learning model.
    '''
    last_time = time.time()
    logs_inferenced_results = []
    log_templates_modified = []
    while True:
        logs_payload = await incoming_logs_queue.get()
        start_time = time.time()
        logging.info("Received payload of size {}".format(len(logs_payload.items)))
        cp_model_logs = []
        rancher_model_logs = []
        longhorn_model_logs = []
        for log_data in logs_payload.items:
            log_message = log_data.masked_log
            if log_message:
                pretrained_template_matched, pretrained_template_payload = match_template(log_data, pretrained_template_miner)
                if pretrained_template_matched:
                    logs_inferenced_results.append(log_data)
                    if pretrained_template_payload:
                        log_templates_modified.append(pretrained_template_payload)
                else:
                    current_template_matched, current_template_payload = match_template(log_data, current_template_miner)
                    if current_template_matched:
                        logs_inferenced_results.append(log_data)
                        if current_template_payload:
                            log_templates_modified.append(current_template_payload)
                    else:
                        if log_data.log_type == "controlplane":
                            cp_model_logs.append(log_data)
                        elif log_data.log_type == "rancher":
                            rancher_model_logs.append(log_data)
                        elif log_data.log_type == "longhorn":
                            longhorn_model_logs.append(log_data)
        if (start_time - last_time >= 1) or len(logs_inferenced_results) >= 128 or len(log_templates_modified) >= 128:
            if len(logs_inferenced_results) > 0:
                await nw.publish("inferenced_logs", bytes(PayloadList(items=logs_inferenced_results)))
                logs_inferenced_results = []
            if len(log_templates_modified) > 0:
                await nw.publish("templates_index", bytes(PayloadList(items=log_templates_modified)))
                log_templates_modified = []
            last_time = start_time
        if len(cp_model_logs) > 0:
            await nw.publish("opnilog_cp_logs", bytes(PayloadList(items = cp_model_logs)))
            logging.info(f"Published {len(cp_model_logs)} logs to be inferenced on by Control Plane Deep Learning model.")
        if len(rancher_model_logs) > 0:
            await nw.publish("opnilog_rancher_logs", bytes(PayloadList(items = rancher_model_logs)))
            logging.info(f"Published {len(rancher_model_logs)} logs to be inferenced on by Rancher Deep Learning model.")
        if len(longhorn_model_logs) > 0:
            await nw.publish("opnilog_longhorn_logs", bytes(PayloadList(items = longhorn_model_logs)))
            logging.info(f"Published {len(longhorn_model_logs)} logs to be inferenced on by Longhorn Deep Learning model.")
        logging.info(f"{len(logs_payload.items)} logs processed in {(time.time() - start_time)} second(s)")


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
        await nw.publish("batch_processed", "processed".encode())
        if len(template_data) > 0:
            await nw.publish("templates_index", bytes(PayloadList(items=template_data)))

async def init_nats():
    # This function initialized the connection to Nats.
    logging.info("connecting to nats")
    await nw.connect()




def main():
    loop = asyncio.get_event_loop()
    incoming_cp_logs_queue = asyncio.Queue(loop=loop)
    update_model_logs_queue = asyncio.Queue(loop=loop)
    batch_processed_queue = asyncio.Queue(loop=loop)
    init_nats_task = loop.create_task(init_nats())
    loop.run_until_complete(init_nats_task)
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