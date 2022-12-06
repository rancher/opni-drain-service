# Third Party
from opni_proto.log_anomaly_payload_pb import Payload, PayloadList

# Local
from drain_service import drain_training_inferencing as dt
from drain_service.drain3.template_miner import TemplateMiner


def test_inference_logs(mocker):
    logs_payload = PayloadList([
        Payload(masked_log="log message 1", log="log1"),
        Payload(masked_log="log message 2", log="log2"),
    ])
    workload_template_miner = TemplateMiner()
    logs_inferenced_results = []
    log_templates_modified = []

    # test failure match
    expected_workload_model_logs = [
        Payload(masked_log="log message 1", log="log1"),
        Payload(masked_log="log message 2", log="log2"),
    ]
    result = dt.inference_logs(logs_payload, workload_template_miner, logs_inferenced_results, log_templates_modified)

    assert len(result) == len(expected_workload_model_logs)
    for p1, p2 in zip(result, expected_workload_model_logs):
        assert p1.masked_log == p2.masked_log
    assert logs_inferenced_results == []
    assert log_templates_modified == []

    # test success match
    mocker.patch("drain_service.drain3.template_miner.TemplateMiner.match", return_value={
            'template': True,
            'anomaly_level': 'anomaly',
            'template_cluster_id': 1234,
            'template_log': 'log message *'
        })

    result_suc = dt.inference_logs(logs_payload, workload_template_miner, logs_inferenced_results, log_templates_modified)
    expected_workload_model_logs_suc = []
    assert result_suc == []
    assert len(result_suc) == len(expected_workload_model_logs_suc)

    assert len(logs_inferenced_results) == 2
    assert len(log_templates_modified) == 2
