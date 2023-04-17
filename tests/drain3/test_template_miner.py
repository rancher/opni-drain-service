# Third Party
import pytest

# Local
from drain_service.drain3.file_persistence import FilePersistence
from drain_service.drain3.template_miner import TemplateMiner
from drain_service.drain3.template_miner_config import TemplateMinerConfig


@pytest.fixture
def template_miner(mocker):
    mocker.patch("drain_service.drain3.file_persistence.FilePersistence.connect_to_s3", return_value=None)
    persistence_handler = FilePersistence("test")
    config = TemplateMinerConfig()
    clusters_counter = 0
    yield TemplateMiner(persistence_handler, config, clusters_counter)

@pytest.fixture
def test_data():
    data = [
        {
            "masked_log" : 'there are <NUM> test message',
            "log" : 'there are 2 test message',
            "anomaly_level" : "anomaly",
            "pretrained" : True,
        },
        {
            "masked_log" : 'there are <NUM> test message',
            "log" : 'there are 3 test message',
            "anomaly_level" : "anomaly",
            "pretrained" : True,
        },
        {
            "masked_log" : 'there are <NUM> new message',
            "log" : 'there are 4 new message',
            "anomaly_level" : "anomaly",
            "pretrained" : True,
        },
        {
            "masked_log" : 'there are <NUM> new message',
            "log" : 'there are 5 new message',
            "anomaly_level" : "anomaly",
            "pretrained" : True,
        },
    ]
    return data

def test_add_log_message(template_miner, test_data):
    data = test_data[0]
    masked_log_message = data["masked_log"]
    original_log_message = data["log"]
    anomaly_level = data["anomaly_level"]
    result = template_miner.add_log_message(masked_log_message, original_log_message, anomaly_level)
    assert result['change_type'] == 'cluster_created'
    assert result['cluster_id'] == 1
    assert result['cluster_size'] > 0
    assert result['template_mined'] == 'there are <NUM> test message'
    assert result['cluster_count'] > 0
    assert result['sample_log'] is not None

def test_add_log_template(template_miner, test_data):
    data = test_data[0]
    log_template = "there are * test message"
    pretrained = data["pretrained"]
    anomaly_level = data["anomaly_level"]
    result = template_miner.add_log_template(log_template, pretrained, anomaly_level)
    assert result['cluster_id'] == 1
    assert result['cluster_size'] > 0
    assert result['template_mined'] == "there are * test message"
    assert result['cluster_count'] > 0

def test_match(template_miner, test_data):
    data = test_data[0]
    masked_log_message = data["masked_log"]
    log_message = data["log"]

    # test match failure
    result = template_miner.match(masked_log_message, log_message)
    assert result["template"] is None

    # test match success
    template_miner.add_log_message(masked_log_message, log_message, data["anomaly_level"])
    result_new = template_miner.match(masked_log_message, log_message)
    assert result_new["template"] is not None



def test_get_parameter_list(template_miner):
    log_template = 'test message'
    content = 'test content'
    result = template_miner.get_parameter_list(log_template, content)
    assert result is not None

def test_with_batch_data(template_miner, test_data):
    data0 = test_data[0]
    r0_match = template_miner.match(data0["masked_log"], data0["log"])
    assert r0_match["template"] is None
    r0_add_log = template_miner.add_log_message(data0["masked_log"], data0["log"], data0["anomaly_level"])
    assert r0_add_log["change_type"] == "cluster_created"
    assert r0_add_log["cluster_id"] == 1
    # r0_add_template = template_miner.add_log_template(r0_add_log["template_mined"] ,data0["pretrained"], data0["anomaly_level"])
    # assert r0_add_template["cluster_id"] == 2

    data1 = test_data[1]
    r1_match = template_miner.match(data1["masked_log"], data1["log"])
    assert r1_match["template"] is not None

    data2 = test_data[2]
    r2_match = template_miner.match(data2["masked_log"], data2["log"])
    assert r2_match["template"] is None
    r2_add_log = template_miner.add_log_message(data2["masked_log"], data2["log"], data2["anomaly_level"])
    assert r2_add_log["change_type"] == 'cluster_template_changed'
    assert r2_add_log["cluster_id"] == 1

    data3 = test_data[3]
    r3_match = template_miner.match(data3["masked_log"], data3["log"])
    assert r3_match["template"] is not None
