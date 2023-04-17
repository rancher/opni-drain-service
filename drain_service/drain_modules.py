import os
import drain_pretrained_inferencing
import drain_training_inferencing

IS_PRETRAINED_SERVICE = os.getenv("IS_PRETRAINED_SERVICE", "true")

if __name__ == "__main__":
    if IS_PRETRAINED_SERVICE == "true":
        drain_pretrained_inferencing.main()
    else:
        drain_training_inferencing.main()

