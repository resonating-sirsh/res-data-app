from webcam import get_latest_webcam_paths
from similarity import get_best_matches, update_weaviate
from res.utils import logger
import warnings

warnings.simplefilter(action="ignore", category=ResourceWarning)

# times we have pumped up the jam: 4
if __name__ == "__main__":
    update_weaviate()
    webcam_paths = list(get_latest_webcam_paths())
    num_webcam_paths = len(webcam_paths)
    if num_webcam_paths > 0:
        logger.info(f"Detected {num_webcam_paths} new webcam images: {webcam_paths}")
        best_matches = get_best_matches(webcam_paths)
    else:
        logger.info("No new webcam images detected.")
