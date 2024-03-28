from datetime import datetime, timedelta
from dataclasses import dataclass


# -- station run times
PRETREAT_START = timedelta(hours=6, minutes=10)
PRETREAT_END = timedelta(hours=15)
PRINT_START = timedelta(hours=7)
PRINT_END = timedelta(hours=15, minutes=30)
STEAM_START = timedelta(hours=9, minutes=15)
STEAM_END = timedelta(hours=18)
WASH_START = timedelta(hours=9, minutes=30)
WASH_END = timedelta(hours=18)
DRY_START = timedelta(hours=9, minutes=45)
DRY_END = timedelta(hours=18)
INSPECT_1_START = timedelta(hours=10)
INSPECT_1_END = timedelta(hours=19)


@dataclass
class TimeWindow:
    start: datetime
    end: datetime


@dataclass
class StationRuntimes:
    day_start: datetime
    pretreat: TimeWindow
    print: TimeWindow
    steam: TimeWindow
    wash: TimeWindow
    dry: TimeWindow
    inspect_1: TimeWindow


def get_station_runtimes(schedule_date):
    t0 = datetime(schedule_date.year, schedule_date.month, schedule_date.day)
    is_friday = t0.isoweekday() == 5
    end_hour_delta = timedelta(hours=-1) if is_friday else timedelta(hours=0)
    return StationRuntimes(
        day_start=t0,
        pretreat=TimeWindow(
            start=t0 + PRETREAT_START,
            end=t0 + PRETREAT_END + end_hour_delta,
        ),
        print=TimeWindow(
            start=t0 + PRINT_START,
            end=t0 + PRINT_END + end_hour_delta,
        ),
        steam=TimeWindow(
            start=t0 + STEAM_START,
            end=t0 + STEAM_END + end_hour_delta,
        ),
        wash=TimeWindow(
            start=t0 + WASH_START,
            end=t0 + WASH_END + end_hour_delta,
        ),
        dry=TimeWindow(
            start=t0 + DRY_START,
            end=t0 + DRY_END + end_hour_delta,
        ),
        inspect_1=TimeWindow(
            start=t0 + INSPECT_1_START,
            end=t0 + INSPECT_1_END + end_hour_delta,
        ),
    )
