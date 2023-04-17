from drain_service.drain3 import file_persistence

def test_class_FilePersistence(mocker):
    mocker.patch("drain_service.drain3.file_persistence.FilePersistence.connect_to_s3", return_value=None)
    fp = file_persistence.FilePersistence("test_file")
