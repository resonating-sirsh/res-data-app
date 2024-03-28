import os

from itertools import combinations, product
from math import ceil, floor

from ortools.linear_solver import pywraplp
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp

from res.utils import logger

from src.objective import roll_value, solution_objective
from src.utils import statsd_incr

# from src.objective import asset_value, roll_affinity

OPTIMIZE_SECONDS = int(os.environ.get("OPTIMIZE_SECONDS", 100))

# see https://github.com/google/or-tools/blob/stable/ortools/linear_solver/linear_solver.proto#L571=
MPSOLVER_MODEL_INVALID = 5

SOLVER_STATUSES_MIP = {
    pywraplp.Solver.OPTIMAL: "optimal",
    pywraplp.Solver.FEASIBLE: "feasible",
    pywraplp.Solver.INFEASIBLE: "infeasible",
    pywraplp.Solver.UNBOUNDED: "unbounded",
    pywraplp.Solver.ABNORMAL: "abnormal",
    MPSOLVER_MODEL_INVALID: "model_invalid",
    pywraplp.Solver.NOT_SOLVED: "not_solved",
}

SOLVER_STATUSES_CVRP = {
    0: "routing_not_solved",
    1: "routing_success",
    2: "routing_fail",  # No solution found to the problem.
    3: "routing_fail_timeout",  # Time limit reached before finding a solution.
    4: "routing_invalid",  # Model, model parameters, or flags are not valid
}


"""
1. Get current print queue by printer
2. Compute hours in current queue by printer
3. Compute target hours remaining by printer (~ 10 rolls)
4. Compute estimated time to print by roll (switch time + print time)
5. Model switch cost between rolls to assign
6. Model switch cost between rolls and the last roll in the current queue

"""


def order_print_queue(assigned_rolls):
    logger.info("ordering print queues")
    ordered_queue = sorted(assigned_rolls, key=lambda x: (x[1], x[2]))

    return ordered_queue


def roll_printer_assignment(
    rolls, printers, assets, solver_settings, verbose_solver_logging=False
):
    logger.info(f"Running roll printer assignment with settings {solver_settings}")

    best_status, best_obj, best_solution = _roll_printer_assignment_heuristic(
        rolls, printers, assets, solver_settings, verbose_solver_logging
    )

    for solver_function in (
        _roll_printer_assignment_cvrp,
        _roll_printer_assignment_mip,
    ):
        new_status, new_obj, new_solution = solver_function(
            rolls, printers, assets, solver_settings, verbose_solver_logging
        )
        if new_obj > best_obj:
            best_status, best_obj, best_solution = new_status, new_obj, new_solution

    return best_status, best_obj, best_solution


def _roll_printer_assignment_heuristic(
    rolls, printers, assets, solver_settings, verbose_solver_logging=False
):
    logger.info(f"find naive solution to roll printer assignment")

    rolls_copy = rolls.copy()
    solution = []
    for (
        printer_name,
        printer,
    ) in printers.iterrows():
        # Query rolls table for rolls that are of one of the materials in the printer accepted materials list
        # Filtering rolls by the printer's material capabilitites as well as not being assigned to a printer
        suitable_new_rolls = rolls_copy[
            rolls_copy["material_code"].isin(printer["material_capabilities"])
            & rolls_copy["assigned_printer_name"].isnull()
        ].copy()
        # Sort this table by:
        # 1. being in the same material as an existing roll in that printer's queue
        # 2. roll_value(roll, assets, solver_settings)

        print_queue_remaining_yards = printer["print_queue_remaining_yards"]
        current_print_queue = printer["current_print_queue_roll_ids"]

        # Getting unique materials in order to sort the list
        current_print_queue_materials = list(
            set(printer["current_print_queue_materials"])
        )

        suitable_new_rolls["is_material_in_queue"] = suitable_new_rolls[
            "material_code"
        ].map(lambda x: x in current_print_queue_materials)
        suitable_new_rolls.sort_values(
            by=["is_material_in_queue", "is_prioritized"], ascending=False, inplace=True
        )

        suitable_new_roll_ids = suitable_new_rolls.index.to_list()

        while print_queue_remaining_yards > 0 and suitable_new_roll_ids:
            # 1. Pop first roll off the ordered list of suitable rolls
            new_roll_id = suitable_new_roll_ids.pop()
            new_roll_material = suitable_new_rolls.loc[new_roll_id, "material_code"]
            # 2. Find the best spot in the queue to place it (find the last roll in the same material and place it after, or append it at the end)
            try:
                rindex = max(
                    i
                    for i, rid in enumerate(current_print_queue)
                    if rolls_copy.loc[rid, "material_code"] == new_roll_material
                )
                current_print_queue = (
                    current_print_queue[: rindex + 1]
                    + [new_roll_id]
                    + current_print_queue[rindex + 1 :]
                )
            except:
                current_print_queue += [new_roll_id]
            # 3. Mark the newly assigned roll as assigned to it does not get assigned to other printers
            rolls_copy.loc[new_roll_id, "assigned_printer_name"] = printer_name
            # 4. Recompute the print queue remaining yards
            print_queue_remaining_yards -= suitable_new_rolls.loc[
                new_roll_id, "total_nested_length_yards"
            ]

        solution += zip(
            current_print_queue,
            [printer_name] * len(current_print_queue),
            range(1, len(current_print_queue) + 1),
        )

    obj = solution_objective(solution, rolls, assets, solver_settings)

    return "solved_heuristic", obj, solution


def printer_length_capacity(printer, solver_settings):
    if printer.print_queue_remaining_yards == 0:
        return ceil(printer["current_print_queue_yards"])
    if solver_settings["constraints"]["keep_current_queue"]:
        return max(
            solver_settings["constraints"]["max_print_queue_yards"],
            ceil(printer["current_print_queue_yards"]),
        )
    return solver_settings["constraints"]["max_print_queue_yards"]


def create_data_model_cvrp(rolls, printers, assets, solver_settings):
    data = dict()

    data["start_node"] = 0
    data["roll_ids"] = rolls.index.to_list()
    data["node_names"] = ["start_node"] + data["roll_ids"]

    data["unassigned_roll_ids"] = rolls.query(
        "assigned_printer_name.isnull()"
    ).index.to_list()
    data["printer_names"] = printers.index.to_list()

    data["num_printers"] = len(data["printer_names"])
    data["num_rolls"] = len(data["roll_ids"])
    data["num_nodes"] = len(data["node_names"])

    data["printers_by_material"] = (
        printers.reset_index()
        .explode("material_capabilities")
        .groupby("material_capabilities")
        .printer_name.agg(list)
        .to_dict()
    )

    data["current_print_queue_solution"] = [
        [
            data["node_names"].index(rid)
            for rid in printers.loc[p, "current_print_queue_roll_ids"]
        ]
        for p in data["printer_names"]
    ]

    data["switch_cost_matrix"] = [[0] * len(data["node_names"])] + [
        [0]
        + [
            int(
                rolls.loc[r_left, "material_code"]
                != rolls.loc[r_right, "material_code"]
            )
            for r_left in data["roll_ids"]
        ]
        for r_right in data["roll_ids"]
    ]
    data["node_drop_penalties"] = [0] + [
        int(roll_value(rolls.loc[r], assets, solver_settings)) for r in data["roll_ids"]
    ]

    # Capacities & demands (set limit for print queue length)
    data["printer_length_capacities"] = [
        printer_length_capacity(printers.loc[p], solver_settings)
        for p in data["printer_names"]
    ]

    data["node_length_demands"] = [0] + [
        floor(rolls.loc[r, "total_nested_length_yards"]) for r in data["roll_ids"]
    ]

    logger.debug(data)

    return data


def format_solution_cvrp(data, manager, routing, assignment):
    """Prints assignment on console."""
    status_num = routing.status()
    status = SOLVER_STATUSES_CVRP[status_num]
    logger.info(f"solver status: {status}")

    if not assignment:
        return status, 0, []

    obj_solv = assignment.ObjectiveValue()
    logger.info(f"solver objective: {obj_solv}")

    # Display dropped nodes.
    dropped_nodes = "Dropped nodes:"
    for node in range(routing.Size()):
        if routing.IsStart(node) or routing.IsEnd(node):
            continue
        if assignment.Value(routing.NextVar(node)) == node:
            dropped_nodes += " {}".format(manager.IndexToNode(node))
    logger.debug(dropped_nodes)

    # Display routes
    total_cost = 0
    total_queue_length = 0
    solution_nodes = []
    for printer_id, printer_name in enumerate(data["printer_names"]):
        index = routing.Start(printer_id)
        plan_output = "Schedule for printer {}:\n".format(printer_name)
        printer_cost = 0
        printer_queue_length = 0
        printer_queue_nodes = []
        while not routing.IsEnd(index):
            node_index = manager.IndexToNode(index)
            printer_queue_length += data["node_length_demands"][node_index]
            plan_output += " {0} Length ({1}) -> ".format(
                node_index, printer_queue_length
            )
            previous_index = index
            index = assignment.Value(routing.NextVar(index))
            printer_cost += routing.GetArcCostForVehicle(
                previous_index, index, printer_id
            )
            printer_queue_nodes.append(data["node_names"][node_index])
        plan_output += " {0} Length ({1})\n".format(
            manager.IndexToNode(index), printer_queue_length
        )
        plan_output += "Cost of the Printer Schedule: {}\n".format(printer_cost)
        plan_output += "Length of the Printer Queue: {}\n".format(printer_queue_length)
        logger.debug(plan_output)

        total_cost += printer_cost
        total_queue_length += printer_queue_length

        solution_nodes.extend(
            list(
                zip(
                    printer_queue_nodes,
                    [printer_name] * len(printer_queue_nodes),
                    range(len(printer_queue_nodes)),
                )
            )
        )
    logger.info("Total Cost of all queues: {}".format(total_cost))
    logger.info("Total Length of all queues: {}".format(total_queue_length))

    solution = [t for t in solution_nodes if "start_node" not in t]

    return status, obj_solv, solution


def _roll_printer_assignment_cvrp(
    rolls, printers, assets, solver_settings, verbose_solver_logging=False
):
    data = create_data_model_cvrp(rolls, printers, assets, solver_settings)
    logger.info("use capacitated vehicle routing solver")
    # Create the routing index manager.
    manager = pywrapcp.RoutingIndexManager(
        data["num_nodes"], data["num_printers"], data["start_node"]
    )

    # Create Routing Model.
    routing = pywrapcp.RoutingModel(manager)

    # Create and register a transit callback.
    def switch_cost_callback(from_index, to_index):
        """Returns the switch cost between the two rolls."""
        # Convert from routing variable Index to switch cost matrix NodeIndex.
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        return data["switch_cost_matrix"][from_node][to_node]

    transit_callback_index = routing.RegisterTransitCallback(switch_cost_callback)

    # Define cost of each arc.
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    # Add Capacity constraint.
    def demand_callback(from_index):
        """Returns the demand of the roll."""
        # Convert from routing variable Index to demands NodeIndex.
        from_node = manager.IndexToNode(from_index)
        return data["node_length_demands"][from_node]

    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)

    routing.AddDimensionWithVehicleCapacity(
        demand_callback_index,
        5,  # capacity slack
        data["printer_length_capacities"],  # printer maximum capacities
        True,  # start cumul to zero
        "Capacity",
    )

    for node, roll_id in enumerate(data["roll_ids"], start=1):
        # Allow to drop nodes.
        logger.debug(
            f"Cost to drop roll {roll_id}: {data['node_drop_penalties'][node]}"
        )
        routing.AddDisjunction(
            [manager.NodeToIndex(node)], data["node_drop_penalties"][node]
        )

        # Only allow roll to be assigned to printers with correct material capabilities
        # unless the roll has been assigned to a printer already and must be kept
        if not (
            solver_settings["constraints"]["keep_current_queue"]
            and rolls.loc[roll_id, "assigned_printer_name"]
        ):
            mat = rolls.loc[roll_id, "material_code"]
            printer_ids = [
                data["printer_names"].index(p)
                for p in data["printers_by_material"][mat]
            ]
            logger.info(
                f"only allow roll {roll_id} to be assigned to printers {data['printers_by_material'][mat]}"
            )
            logger.debug(f"node {node} to printers {printer_ids}")
            routing.VehicleVar(node).SetValues(printer_ids)

    # Fix existing assignments
    if solver_settings["constraints"]["keep_current_queue"]:
        for printer_id, printer_name in enumerate(data["printer_names"]):
            for roll_id in printers.loc[printer_name, "current_print_queue_roll_ids"]:
                logger.info(
                    f"keep existing assignment of roll {roll_id} to printer {printer_name}"
                )
                node = data["node_names"].index(roll_id)
                routing.VehicleVar(node).SetValues([printer_id])

    initial_solution = routing.ReadAssignmentFromRoutes(
        data["current_print_queue_solution"], True
    )

    # Setting first solution heuristic.
    search_parameters = pywrapcp.DefaultRoutingSearchParameters()
    search_parameters.first_solution_strategy = (
        routing_enums_pb2.FirstSolutionStrategy.PATH_MOST_CONSTRAINED_ARC
    )
    search_parameters.local_search_metaheuristic = (
        routing_enums_pb2.LocalSearchMetaheuristic.AUTOMATIC  # GUIDED_LOCAL_SEARCH
    )
    search_parameters.time_limit.FromSeconds(OPTIMIZE_SECONDS)

    # Solve the problem.
    logger.info("Start solving")
    assignment = routing.SolveFromAssignmentWithParameters(
        initial_solution, search_parameters
    )

    status, obj_solv, solution = format_solution_cvrp(
        data, manager, routing, assignment
    )
    obj = solution_objective(solution, rolls, assets, solver_settings)

    return status, obj, solution


def _roll_printer_assignment_mip(
    rolls, printers, assets, solver_settings, verbose_solver_logging=False
):
    logger.info("use MIP solver")
    solver = pywraplp.Solver.CreateSolver("CP-SAT")

    if verbose_solver_logging:
        logger.info("running solver in verbose logging mode")
        solver.EnableOutput()

    roll_ids = rolls.index.to_list()
    unassigned_roll_ids = rolls.query("assigned_printer_name.isnull()").index.to_list()

    logger.debug(
        "considering a total of %d rolls (of which %d are currently not queued)"
        % (len(roll_ids), len(unassigned_roll_ids))
    )

    printer_names = printers.index.to_list()
    possible_assignment_slots = list(range(1, len(roll_ids) + 1))

    roll_printer_assignments = {
        (r, p, rnk): solver.BoolVar(f"{r}_{p}_{rnk}")
        for r, p, rnk in product(roll_ids, printer_names, possible_assignment_slots)
    }

    value = solver.NumVar(0, solver.Infinity(), "value")
    cost = solver.NumVar(0, solver.Infinity(), "cost")

    roll_combinations = {}

    statsd_incr("optimus_optimizing", 1)
    statsd_incr("optimus_optimized_rolls", len(rolls))

    ###
    # constraints
    ###

    for r in roll_ids:
        # assign every roll at most once
        solver.Add(
            solver.Sum(
                [
                    roll_printer_assignments[r, p, rnk]
                    for p, rnk in product(printer_names, possible_assignment_slots)
                ]
            )
            <= 1
        )

    for p in printer_names:

        # use every slot in a print queue at most once
        for rnk in possible_assignment_slots:
            solver.Add(
                solver.Sum([roll_printer_assignments[r, p, rnk] for r in roll_ids]) <= 1
            )

        # fill up print queue slots in order
        for rnk_smaller, rnk_greater in combinations(possible_assignment_slots, 2):
            solver.Add(
                solver.Sum(
                    [roll_printer_assignments[r, p, rnk_smaller] for r in roll_ids]
                )
                >= solver.Sum(
                    [roll_printer_assignments[r, p, rnk_greater] for r in roll_ids]
                )
            )

        # add currently assigned rolls to their respective queues
        if solver_settings["constraints"]["keep_current_queue"]:
            logger.info("keep all rolls currently assigned in their printer queue")
            for i, r in enumerate(printers.loc[p, "current_print_queue_roll_ids"]):
                solver.Add(
                    solver.Sum(
                        [
                            roll_printer_assignments[r, p, rnk]
                            for rnk in possible_assignment_slots
                        ]
                    )
                    == 1
                )

                if i == 0:
                    # preserve the place of the first roll in the queue (the rest can be reordered)
                    logger.info(
                        f"keep roll {rolls.loc[r, 'roll_name']} at the front of the queue for {p}"
                    )
                    solver.Add(roll_printer_assignments[r, p, 1] == 1)

        # assign extra rolls to reach at most 1 day's work in the print queue (approx 500 yards)
        # the remaining yards to print after current print queue has been assigned has been pre-computed
        solver.Add(
            solver.Sum(
                [
                    roll_printer_assignments[r, p, rnk]
                    * rolls.loc[r, "total_nested_length_yards"]
                    for r, rnk in product(
                        unassigned_roll_ids, possible_assignment_slots
                    )
                ]
            )
            <= printers.loc[p, "print_queue_remaining_yards"]
        )

        # only assign new rolls with materials in the approved list
        for r in unassigned_roll_ids:
            if (
                rolls.loc[r, "material_code"]
                not in printers.loc[p, "material_capabilities"]
            ):
                for rnk in possible_assignment_slots:
                    solver.Add(roll_printer_assignments[r, p, rnk] == 0)

        for r1, r2 in combinations(roll_ids, 2):
            for rnk in possible_assignment_slots[:-1]:
                roll_combinations[(r1, r2, p, rnk)] = solver.BoolVar(
                    "%s_%s_%s_%d" % (r1, r2, p, rnk)
                )

                # Add constraints to force roll_combinations to equal the product of in-order roll_printer_assignments for two rolls
                # r1 * r2 = comb_1_2
                # Since r1 and r2 are boolean, this can be achieved using the following three relations:
                # comb_1_2 <= r1
                # comb_1_2 <= r2
                # comb_1_2 >= r1 + r2 - 1
                solver.Add(
                    roll_combinations[(r1, r2, p, rnk)]
                    <= (
                        roll_printer_assignments[r1, p, rnk]
                        + roll_printer_assignments[r1, p, rnk + 1]
                    )
                )
                solver.Add(
                    roll_combinations[(r1, r2, p, rnk)]
                    <= (
                        roll_printer_assignments[r2, p, rnk]
                        + roll_printer_assignments[r2, p, rnk + 1]
                    )
                )
                solver.Add(
                    roll_combinations[(r1, r2, p, rnk)]
                    >= (
                        roll_printer_assignments[r1, p, rnk]
                        + roll_printer_assignments[r1, p, rnk + 1]
                    )
                    + (
                        roll_printer_assignments[r2, p, rnk]
                        + roll_printer_assignments[r2, p, rnk + 1]
                    )
                    - 1
                )

    # Set value equal to sum of roll utilization and roll print priority score (both in unit range)
    solver.Add(
        value
        == solver.Sum(
            [
                roll_printer_assignments[r, p, rnk]
                * roll_value(
                    rolls.loc[r],
                    assets.loc[rolls.loc[r, "assigned_print_assets"]],
                    solver_settings,
                )
                for r, p, rnk in product(
                    roll_ids, printer_names, possible_assignment_slots
                )
            ]
        )
    )

    # Set cost equal to material switches times some cost per switch (might become material-type dependent)
    solver.Add(
        cost
        == solver.Sum(
            [
                roll_combinations[(r1, r2, p, rnk)]
                * solver_settings["costs"]["material_switch_cost"]
                for r1, r2 in combinations(roll_ids, 2)
                for p in printer_names
                for rnk in possible_assignment_slots[:-1]
                if rolls.loc[r1, "material_code"] != rolls.loc[r2, "material_code"]
            ]
        )
    )

    # objective function
    solver.Maximize(value - cost)

    # milliseconds
    solver.set_time_limit(OPTIMIZE_SECONDS * 1000)

    logger.info("optimizing assignment of rolls to printers")
    status_num = solver.Solve()
    status = SOLVER_STATUSES_MIP[status_num]
    obj_solv = solver.Objective().Value()
    logger.info("done, took %d milliseconds" % solver.WallTime())
    statsd_incr("optimus_optimizing_time", solver.WallTime())

    if status in ("optimal", "feasible"):

        roll_printer_pairs = [
            (roll_key, printer_name, rnk)
            for roll_key, printer_name, rnk in roll_printer_assignments
            if roll_printer_assignments[roll_key, printer_name, rnk].solution_value()
            == 1
        ]

        roll_printer_pairs_ordered = order_print_queue(roll_printer_pairs)

        logger.info(
            "found an answer! status: %s, objective: %0.3f (value: %0.3f, cost: %0.3f)"
            % (status, obj_solv, value.solution_value(), cost.solution_value())
        )
        statsd_incr("optimus_solution", 1)

        obj = solution_objective(
            roll_printer_pairs_ordered, rolls, assets, solver_settings
        )

        return status, obj, roll_printer_pairs_ordered
    else:
        logger.info("failed to find a solution! status: %s" % status)
        statsd_incr("optimus_no_solution", 1)

        return status, 0, []
