from black import main
from pyzbar.pyzbar import decode
import cv2 as cv
import datetime
import os
import res.utils.premises.premtools as premtools
import schedule
import sys
from res.utils import logger

__VERSION__ = "1.1"
restart = False
cap = cv.VideoCapture(0)
cap.set(cv.CAP_PROP_FRAME_WIDTH, 1920)
cap.set(cv.CAP_PROP_FRAME_HEIGHT, 1080)
scanned_codes = set()
LOCAL_REPO_DIRECTORY = os.getenv("LOCAL_REPO_DIRECTORY")
LOCAL_EVENTS_DIRECTORY = f"{LOCAL_REPO_DIRECTORY}/events"
RES_NAMESPACE = os.getenv("RES_NAMESPACE", "res_premises")
RES_APP_NAME = os.environ["RES_APP_NAME"] = "camera_scanner"
DEVICE_NAME = os.getenv("DEVICE_NAME")


def event(code):
    topic = "dr-factory-events-stream"

    content = {
        "TIMESTAMP": str(datetime.datetime.now()),
        "QRCODE": code,
        "SENSOR_ID": DEVICE_NAME,
    }
    event = premtools.send_to_kgateway(topic, RES_APP_NAME, content)
    premtools.event_to_local(event, LOCAL_EVENTS_DIRECTORY, RES_APP_NAME)


def main():
    schedule.every(5).minutes.do(
        premtools.app_status_notifier, DEVICE_NAME, RES_APP_NAME, "running"
    )
    while True:
        schedule.run_pending()
        ret, frame = cap.read()
        if ret == True:
            image_decoded = decode(frame)

            for qr in image_decoded:
                code = qr.data.decode()
                if code not in scanned_codes:
                    latest_version = premtools.check_version(os.path.realpath(__file__))
                    if len(scanned_codes) < 10:
                        scanned_codes.add(code)
                        event(code)
                        if __VERSION__ != latest_version:
                            logger.info("version is behind. Updating..")
                            os.execv(sys.executable, ["python"] + [sys.argv[0]])

                    else:
                        scanned_codes.clear()

            # frame = cv.resize(frame, (1080, 720)) #can't run with cron.
            # cv.imshow("frame", frame)

            if cv.waitKey(1) == ord("q"):
                break
        else:
            continue

    cap.release()
    cv.destroyAllWindows()
    premtools.app_status_notifier(DEVICE_NAME, RES_APP_NAME, "manual override")


if __name__ == "__main__":

    try:
        premtools.check_version(os.path.realpath(__file__))
        main()

    except Exception as e:
        logger.error(f"An Unknown error has occurred. {e}")
        premtools.app_status_notifier(DEVICE_NAME, RES_APP_NAME, "ended with error")
