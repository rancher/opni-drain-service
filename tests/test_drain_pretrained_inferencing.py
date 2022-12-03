from drain_service import drain_pretrained_inferencing as dp


def test_load_pretrain_model(mocker):
    mocker.patch("drain_service.drain_pretrained_inferencing.FilePersistence.connect_to_s3", return_value=None)
    p, c = dp.load_pretrain_model()
    assert p is not None
    assert c is not None