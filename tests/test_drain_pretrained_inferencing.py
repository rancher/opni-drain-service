# Third Party
import pytest
from opni_proto.log_anomaly_payload_pb import Payload, PayloadList

# Local
from drain_service import drain_pretrained_inferencing as dp
from drain_service.drain3.template_miner import TemplateMiner


def test_load_pretrain_model(mocker):
    mocker.patch("drain_service.drain_pretrained_inferencing.FilePersistence.connect_to_s3", return_value=None)
    p, c = dp.load_pretrain_model()
    assert p is not None
    assert c is not None

## test 'match_template()'

@pytest.fixture
def log_data():
    return Payload(masked_log="masked_log", log="log")


def test_match_template_success(mocker, log_data):
    mocker.patch("drain_service.drain3.template_miner.TemplateMiner.match", return_value={
            'template': True,
            'anomaly_level': 'anomaly',
            'template_cluster_id': 1234,
            'template_log': 'template_log'
        })
    success, template_log_payload = dp.match_template(log_data, TemplateMiner())
    assert success
    assert log_data.anomaly_level == 'anomaly'
    assert log_data.template_cluster_id == 1234
    assert log_data.template_matched is True
    assert log_data.inference_model == 'drain'
    assert template_log_payload.log == 'template_log'
    assert template_log_payload.template_matched is True
    assert template_log_payload.template_cluster_id == 1234
    assert template_log_payload._id == '1234'

def test_match_template_failure(mocker,log_data):
    mocker.patch("drain_service.drain3.template_miner.TemplateMiner.match", return_value={
            'template': False,
        'anomaly_level': None,
        'template_cluster_id': None,
        'template_log': None
        })
    success, template_log_payload = dp.match_template(log_data, TemplateMiner())
    assert not success
    assert template_log_payload is None
    assert log_data.anomaly_level == ""
    assert log_data.template_cluster_id == 0
    assert log_data.template_matched == ""
    assert log_data.inference_model == ""


## test 'inference_logs()'

@pytest.fixture
def logs_payload():
    return PayloadList([
        Payload(log_type= 'controlplane', masked_log= 'controlplane log1', log= "logs1 "),
        Payload(log_type= 'controlplane', masked_log= 'controlplane log2', log= "logs2 "),
        Payload(log_type= 'rancher', masked_log= 'rancher log1', log= "rancher logs1 "),
        Payload(log_type= 'longhorn', masked_log= 'longhorn log1', log= "longhorn logs1 "),
        Payload(log_type= 'longhorn', masked_log= 'longhorn log2', log= "longhorn logs2 "),
    ])

@pytest.fixture
def pretrained_template_miner():
    return TemplateMiner()

@pytest.fixture
def current_template_miner():
    return TemplateMiner()

@pytest.fixture
def logs_inferenced_results():
    return []

@pytest.fixture
def log_templates_modified():
    return []

def test_inference_logs_match_false(logs_payload, pretrained_template_miner, current_template_miner, logs_inferenced_results, log_templates_modified):
    cp_model_logs, rancher_model_logs, longhorn_model_logs = dp.inference_logs(logs_payload, pretrained_template_miner, current_template_miner, logs_inferenced_results, log_templates_modified)
    assert len(cp_model_logs) == 2
    assert len(rancher_model_logs) == 1
    assert len(longhorn_model_logs) == 2
    assert len(logs_inferenced_results) == 0
    assert len(log_templates_modified) == 0

def test_inference_logs_match_true(mocker, logs_payload, pretrained_template_miner, current_template_miner, logs_inferenced_results, log_templates_modified):
    mocker.patch("drain_service.drain3.template_miner.TemplateMiner.match", return_value={
            'template': True,
            'anomaly_level': 'anomaly',
            'template_cluster_id': 1234,
            'template_log': '* log*'
        })
    cp_model_logs, rancher_model_logs, longhorn_model_logs = dp.inference_logs(logs_payload, pretrained_template_miner, current_template_miner, logs_inferenced_results, log_templates_modified)
    assert len(cp_model_logs) == 0
    assert len(rancher_model_logs) == 0
    assert len(longhorn_model_logs) == 0
    assert len(logs_inferenced_results) == 5
    assert len(log_templates_modified) == 5
