import sys
import json

from data import (
    construct_output_messages,
    construct_simulation_settings,
    process_input_data,
)

from simulator import run_simulation
from model import PrintRollModel


def handler(event, context):
    # Process input data
    data = process_input_data(event, context)
    simulation_settings = construct_simulation_settings(event)

    # Run simulation
    simulation_result = run_simulation(data, simulation_settings)

    output = construct_output_messages(simulation_result)

    # Send output to Kafka?


if __name__ == "__main__":
    event = json.loads(sys.argv[1])
    data = json.loads(sys.argv[2])
    NUMBER_OF_RUNS = 5
    # handler(event, data)
    for run in range(NUMBER_OF_RUNS):
        print(f"Simulation #{run+1} of {NUMBER_OF_RUNS}")
        print_model = PrintRollModel()
        print_model.run()
        print("\n\n")
