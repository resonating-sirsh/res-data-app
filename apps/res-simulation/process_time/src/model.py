from enum import Enum
import simpy
import random
import pandas as pd


class Node:
    def __init__(self) -> None:
        pass


class Resource:
    def __init__(self) -> None:
        pass


# Storing Global Paramaters
# Instead of creating an instance, we just access to these paramaters.
class GlobalParamaters:
    # Every how many minutes we have rolls ripped - ready to print
    ROLL_RIPPED_INTER_TIME = 5

    # Shift + Materil per Shift.
    PRINTERS_CONFIGURATION = [
        {
            "PRINTER_NAME": "JP7 LUNA",
            "MATERIAL_CAPABILITIES": [
                "OCTHR",
                "COMCT",
                "ECVIC",
                "CDCBM",
                "CHRST",
                "HC293",
                "CTNSP",
                "FBTRY",
                "CTJ95",
                "LSG19",
                "CTNBA",
            ],
        },
        {
            "PRINTER_NAME": "JP7 FLASH",
            "MATERIAL_CAPABILITIES": [
                "CHRST",
                "GGT16",
                "OCT70",
                "CTNOX",
                "OC135",
                "CT170",
                "CDCBM",
                "CUTWL",
                "SLCTN",
                "PIMA7",
                "SECTN",
                "LY100",
                "LY115",
                "LN135",
                "LTCSL",
                "RYJ01",
                "BCICT",
                "CTW70",
                "OCFLN",
                "LSG19",
            ],
        },
        {
            "PRINTER_NAME": "JP7 NOVA",
            "MATERIAL_CAPABILITIES": [
                "CC254",
                "CTSPR",
                "CT406",
                "CFTGR",
                "STCAN",
                "CTPKT",
                "OCTCJ",
                "CTNBA",
                "CFT97",
                "CTFLN",
                "CTF19",
                "CLTWL",
                "CTNL1",
                "CTNSP",
                "PIQUE",
                "CTJ95",
                "FBTRY",
                "CTNPT",
                "TNCDO",
                "HC293",
                "TNSPJ",
                "CTSPS",
            ],
        },
    ]
    MATERIAL_LIST = [
        "BCICT",
        "CC254",
        "CDCBM",
        "CFT97",
        "CFTGR",
        "CHRST",
        "CLTWL",
        "COMCT",
        "CT170",
        "CT406",
        "CTF19",
        "CTFLN",
        "CTJ95",
        "CTNBA",
        "CTNL1",
        "CTNOX",
        "CTNPT",
        "CTNSP",
        "CTPKT",
        "CTSPR",
        "CTSPS",
        "CTW70",
        "CUTWL",
        "ECVIC",
        "FBTRY",
        "GGT16",
        "HC293",
        "LN135",
        "LSG19",
        "LTCSL",
        "LY100",
        "LY115",
        "OC135",
        "OCFLN",
        "OCT70",
        "OCTCJ",
        "OCTHR",
        "PIMA7",
        "PIQUE",
        "RYJ01",
        "SECTN",
        "SLCTN",
        "STCAN",
        "TNCDO",
        "TNSPJ",
    ]
    NUMBER_OF_PRINT_ROOMS = 3
    PRINTER_PER_PRINT_ROOM = 1
    SIMULATION_DURATION = 960
    NUMBER_OF_PRINTERS = 3
    MEAN_PRINT_TIME = 50
    STDEV_PRINT_TIME = 8
    INITIAL_SIMULATION_TIME = 480


# Enum to represent time
class QueueEventType(Enum):
    ENTERED_QUEUE = 1
    LEFT_QUEUE = 2
    PROCESSING_TIME = 3


# Basic Structure of a roll class. This class will represente
class Roll:
    def __init__(self, id, created_at, material=None, material_type=None, length=None):
        self.id = id
        self.material = material
        self.material_type = material_type
        self.length = length
        self.time_in_print_node = {
            "entered_queue": None,
            "left_queue": None,
            "wait_time": None,
            "processing_time": None,
            "printed_at": None,
            "created_at": created_at,
            "assigned_at": None,
        }
        self.assigned_to = None

    def update_time_in_print(self, type_of_event: QueueEventType, ts):
        # Logging time when the roll joined the queue to be printed
        if type_of_event == QueueEventType.ENTERED_QUEUE:
            self.time_in_print_node["entered_queue"] = ts

        # Logging time when the roll left the queue. Also calculating how many minutes it spent in said queue
        elif type_of_event == QueueEventType.LEFT_QUEUE:
            self.time_in_print_node["left_queue"] = ts
            self.calculate_wait_time()

        # Calculating how many minutes took the printing proccess.
        # Logging time when roll was printed.
        elif type_of_event == QueueEventType.PROCESSING_TIME:
            self.time_in_print_node["printed_at"] = ts
            self.time_in_print_node["processing_time"] = (
                ts - self.time_in_print_node["left_queue"]
            )

    def calculate_wait_time(self):
        self.time_in_print_node["wait_time"] = (
            self.time_in_print_node["left_queue"]
            - self.time_in_print_node["entered_queue"]
        )

    def assign_to_printer(self, printer, assigned_at):
        self.assigned_to = printer
        self.time_in_print_node["assigned_at"] = assigned_at


# Model to print a
class PrintRollModel:
    def __init__(self, log_output=True, sim_id=0):
        # Adding a starting time to the simulation!
        self.env = simpy.Environment(
            initial_time=GlobalParamaters.INITIAL_SIMULATION_TIME
        )

        # Placeholder for roll_id
        self.roll_counter = 0
        self.rolls = []

        # To control if we want to log an output
        self.log_output = log_output

        self.sim_id = sim_id

        # Creating Different Print Rooms
        self.print_rooms = [
            PrintRoom(
                self.env,
                name=printer["PRINTER_NAME"],
                approved_materials=printer["MATERIAL_CAPABILITIES"],
                capacity=GlobalParamaters.PRINTER_PER_PRINT_ROOM,
            )
            for printer in GlobalParamaters.PRINTERS_CONFIGURATION
        ]
        self.printer = simpy.Resource(
            self.env, capacity=GlobalParamaters.NUMBER_OF_PRINTERS
        )

    # Arrival Rolls Generator
    def generate_ready_for_print_rolls(self):
        # Keep Generating rolls
        while True:
            self.roll_counter += 1

            # Creating a new roll
            roll_to_print = Roll(
                id=self.roll_counter,
                created_at=self.env.now,
                material=random.choice(GlobalParamaters.MATERIAL_LIST),
            )

            # Appending the roll object to analyze.
            self.rolls.append(roll_to_print)

            # Send the created roll to print
            self.env.process(self.print_roll(roll_to_print))

            # Randomly sampling the time that a new roll will be ready for print.
            # This will simulate the ONE Node.
            sample_inter_arrival = random.expovariate(
                1.0 / GlobalParamaters.ROLL_RIPPED_INTER_TIME
            )

            # Freeze the function until that time has elapsed
            yield self.env.timeout(sample_inter_arrival)

    # Method to model the processes of printing a roll
    # Needs to be passed a roll who will go through these processes.
    def print_roll(self, roll: Roll):
        roll.update_time_in_print(QueueEventType.ENTERED_QUEUE, self.env.now)

        if self.log_output:
            print(
                f"Roll {roll.id} entered queue at {roll.time_in_print_node['entered_queue']}"
            )

        # Now we need to request a printer
        assigned_print_room = self.assign_print_room(roll)

        if self.log_output:
            print(f"Roll {roll.id} assigned to {assigned_print_room.name}")

        roll.assign_to_printer(assigned_print_room.name, self.env.now)
        with assigned_print_room.printers.request() as req:
            # Freeze until the request for a printer can be met
            yield req

            # Calculate time roll was in the queue to be printed
            roll.update_time_in_print(QueueEventType.LEFT_QUEUE, self.env.now)
            if self.log_output:
                print(
                    f"Roll {roll.id} started to print at {roll.time_in_print_node['left_queue']}"
                )

                print(
                    f"Roll {roll.id} queued for {roll.time_in_print_node['wait_time']} minutes"
                )

            # Randomly sample a time that the roll spent while being printed
            # Right now using a normal distribution but this will change
            # Roll printing time can be the roll.lenght * printer speed... And we can sample a loading roll time.. and sum to that
            sample_printing_time = random.gauss(
                GlobalParamaters.MEAN_PRINT_TIME, GlobalParamaters.STDEV_PRINT_TIME
            )

            # Freeze the function until that time has passed (simulating that the roll is being printed)
            yield self.env.timeout(sample_printing_time)
            roll.update_time_in_print(QueueEventType.PROCESSING_TIME, self.env.now)

    # Here is where we are going to select an optimal printer,
    # Simulating Printer Assignment
    def assign_print_room(self, roll: Roll):

        # TODO:
        # ADD Shift.
        shuffled = list(
            zip(range(len(self.print_rooms)), self.print_rooms)
        )  # tuples of (i, line)

        #  Printers are shuffled so that the first queue is not disproportionally selected
        random.shuffle(shuffled)
        shortest_queue = shuffled[0][0]

        # Selecting the Print Room with the Shortest Queue and that accepts the roll's material.
        # This can be improved
        for i, print_room in shuffled:
            if (
                len(print_room.printers.queue)
                <= len(self.print_rooms[shortest_queue].printers.queue)
                and roll.material in print_room.approved_materials
            ):
                shortest_queue = i
        return self.print_rooms[shortest_queue]

    # Function that returns the dataFrame
    def get_rolls_data(self, data_type="dataframe"):
        # Return the list of objects
        if data_type == "list":
            return self.rolls

        # Returns a Dataframe
        elif data_type == "dataframe":
            # Adding a new dataframe to store the rolls
            historical_rolls_df = pd.DataFrame(
                columns=[
                    "sim_id",
                    "roll_id",
                    "created_at",
                    "material",
                    "assigned_at",
                    "entered_print_queue_at",
                    "left_print_queue_at",
                    "minutes_waiting_in_queue",
                    "printed_at",
                    "processing_time",
                    "printed_assigned_to",
                ]
            )
            for roll in self.rolls:
                # Appending the roll data to a dataframe
                historical_rolls_df = historical_rolls_df.append(
                    {
                        "sim_id": self.sim_id,
                        "roll_id": roll.id,
                        "created_at": roll.time_in_print_node["created_at"],
                        "material": roll.material,
                        "assigned_at": roll.time_in_print_node["assigned_at"],
                        "entered_print_queue_at": roll.time_in_print_node[
                            "entered_queue"
                        ],
                        "left_print_queue_at": roll.time_in_print_node["left_queue"],
                        "minutes_waiting_in_queue": roll.time_in_print_node[
                            "wait_time"
                        ],
                        "printed_at": roll.time_in_print_node["printed_at"],
                        "processing_time": roll.time_in_print_node["processing_time"],
                        "printed_assigned_to": roll.assigned_to,
                    },
                    ignore_index=True,
                )

            return historical_rolls_df

    # This method will kickoff the simulation and will tell simpy to start running the environment.
    def run(self):
        self.env.process(self.generate_ready_for_print_rolls())
        self.env.run(until=GlobalParamaters.SIMULATION_DURATION)


# PrintRoom model that will simulate a the operations of a Print Room in Resonance.
#
class PrintRoom:
    def __init__(
        self,
        env,
        name,
        capacity=1,
        approved_materials=None,
        shift_starts=None,
        shift_ends=None,
    ):
        self.name = name
        self.capacity = capacity
        self.approved_materials = approved_materials
        self.shift_starts = shift_starts
        self.shift_ends = shift_ends
        self.printers = simpy.Resource(env, capacity=capacity)

    def set_shifts(self, shift_starts, shift_ends):
        self.shift_starts = shift_starts
        self.shift_ends = shift_ends

    def set_approved_materials(self, approved_material_list):
        self.approved_materials = approved_material_list


if __name__ == "__main__":
    NUMBER_OF_RUNS = 1
    # handler(event, data)
    for run in range(NUMBER_OF_RUNS):
        print(f"Simulation #{run+1} of {NUMBER_OF_RUNS}")
        print_model = PrintRollModel(log_output=False)
        print_model.run()
        x = print_model.get_rolls_data()
        print(x)
        print("\n\n")


# class Scheduler(object):
#     def __init__(self, res, env):
#         self.env = env

#         # Begin the scheduling process method (generator)
#         # This function passes a resource and the environment in its parameters
#         self.process = env.process(self.resource_scheduler(res, env))

#     def resource_scheduler(self, res, env):
#         """
#         This function "closes" resources before "opening_time" and after "closing_time"
#         """
#         while True:
#             if (
#                 self.env.now == 0
#             ):  # When the simulation begins we want a "scheduler" to occupy the resource
#                 try:
#                     with res.spots.request(
#                         priority=1, preempt=True
#                     ) as request:  # Generate resource request event
#                         yield request  # Wait for access to resource
#                         print("%s closes at %d" % (res.name, env.now))
#                         yield self.env.timeout(
#                             res.opening_time
#                         )  # Keep the resource closed until resource "opening time"
#                         print("%s opens at %d" % (res.name, env.now))
#                 except:
#                     pass
#             elif (
#                 self.env.now == res.opening_time
#             ):  # When the simulation reaches the resources "opening time"
#                 try:
#                     open = (
#                         res.closing_time - res.opening_time
#                     )  # Obtain the duration of time resource is open
#                     yield self.env.timeout(
#                         open
#                     )  # Pass time in simulation while resource is open
#                 except:
#                     pass
#             elif (
#                 self.env.now == res.closing_time
#             ):  # When the simulation reaches the resources "closing time"
#                 try:
#                     with res.spots.request(
#                         priority=1, preempt=True
#                     ) as request:  # Generate resource request event
#                         yield request  # Wait for access to resource
#                         print("%s closes at %d" % (res.name, env.now))
#                         close = (
#                             GlobalParamaters.SIMULATION_DURATION - res.closing_time
#                         )  # Obtain duration of time the resource is closed
#                         yield self.env.timeout(
#                             close
#                         )  # Keep resource "closed" until the end of the simulation
#                 except:
#                     pass
#             else:
#                 break
