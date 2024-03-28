"""
Roll prioritization:
* days since pretreatment
* roll utilization?
* roll marked as prioritized for print

Asset prioritization:
* healing requests
* days since order
* order marked as important

Roll-roll affinity:
* material switch cost

Roll-printer affinity:
"""


def asset_value(asset, solver_settings):
    return (
        solver_settings["costs"]["daily_order_delay_cost"]
        * (1 + asset["days_since_ordered"])
        * asset["emphasis"]
    )


def roll_value(roll, assets, solver_settings):
    return (
        roll["roll_utilization_nested_assets"]
        + roll["days_since_pretreatment"]
        * solver_settings["costs"]["roll_expiration_cost"]
        + solver_settings["costs"]["prioritized_roll_value"] * roll["is_prioritized"]
        + sum(
            [
                asset_value(assets.loc[a], solver_settings)
                for a in roll["assigned_print_assets"]
            ]
        )
    )


def solution_value(solution, rolls, assets, solver_settings):
    return sum(
        roll_value(rolls.loc[r], assets, solver_settings) for (r, _, _) in solution
    )


def solution_switch_cost(solution, rolls, solver_settings):
    switch_cost = 0
    printer_prev = None
    material_prev = None
    for r, p, _ in solution:
        if p == printer_prev and rolls.loc[r, "material_code"] != material_prev:
            switch_cost += solver_settings["costs"]["material_switch_cost"]

    return switch_cost


def solution_objective(solution, rolls, assets, solver_settings):
    value = solution_value(solution, rolls, assets, solver_settings)
    cost = solution_switch_cost(solution, rolls, solver_settings)

    return value - cost


# def roll_affinity(roll1, roll2, columns=None):
#     columns = columns or []

#     summed_affinities = sum(all(roll1[col] == roll2[col] for col in columns))

#     summed_affinities_unit_range = 1 / (1 + np.exp(-summed_affinities))

#     return summed_affinities_unit_range
