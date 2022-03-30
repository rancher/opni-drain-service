import os
import drain_cp_inferencing
import drain_training_inferencing

IS_CONTROL_PLANE_SERVICE = os.getenv("IS_CONTROL_PLANE_SERVICE", "true")

if __name__ == "__main__":
    if IS_CONTROL_PLANE_SERVICE == "true":
        drain_cp_inferencing.main()
    else:
        drain_training_inferencing.main()

