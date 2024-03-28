from pynput import keyboard
import res.utils.premises.premtools as premtools
import os
from res.utils.logging.ResLogger import ResLogger
import datetime
import schedule

keys_pressed = []
LOCAL_REPO_DIRECTORY = os.getenv("LOCAL_REPO_DIRECTORY")
LOCAL_EVENTS_DIRECTORY = f"{LOCAL_REPO_DIRECTORY}/events"
RES_ENV = os.getenv("RES_ENV", "development")
RES_NAMESPACE = os.getenv("RES_NAMESPACE", "res_premises")
RES_APP_NAME = os.environ["RES_APP_NAME"] = "handheld_scanner"
DEVICE_NAME = os.getenv("DEVICE_NAME")
NAS_DIRECTORY = f"{DEVICE_NAME}/{RES_APP_NAME}"
logger = ResLogger()
##


def content(code):
    topic = "dr-factory-events-stream"

    content = {
        "TIMESTAMP": str(datetime.datetime.now()),
        "QRCODE": code,
        "SENSOR_ID": DEVICE_NAME,
    }

    event = premtools.send_to_kgateway(topic, RES_APP_NAME, content)
    premtools.event_to_local(event, LOCAL_EVENTS_DIRECTORY, RES_APP_NAME)


def on_press(key):
    try:
        if key != keyboard.Key.enter:
            keys_pressed.append(key.char)
        else:
            qrcode = "".join(keys_pressed)
            print(qrcode)
            content(qrcode)
            del keys_pressed[:]
    except AttributeError:
        # print(f"special key {key} pressed")
        pass


def main():
    listener = keyboard.Listener(on_press=on_press)
    listener.start()
    schedule.every(5).minutes.do(
        premtools.app_status_notifier, DEVICE_NAME, RES_APP_NAME, "running"
    )
    while True:
        schedule.run_pending()


if __name__ == "__main__":
    main()
