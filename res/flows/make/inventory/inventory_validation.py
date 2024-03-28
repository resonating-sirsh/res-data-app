from res.flows.make.inventory.rolls_loader import RollLoader
from schemas.pydantic.make import (
    OrderInfoRequest,
    OrderMaterialInfo,
    OrderMaterialInfoResponse,
    MaterialInfo,
)
from datetime import datetime, timedelta


def get_material_info_for_code_or_name(
    material_code_or_name: str, roll_loader: RollLoader
):
    # some logic in case someone searches using the name instead of the code
    tmp_material_code = roll_loader.get_material_code(material_code_or_name)
    if tmp_material_code is not None:
        material_name = material_code_or_name
        material_code = tmp_material_code
    else:
        material_code = material_code_or_name
        material_name = roll_loader.get_material_name(material_code_or_name)
    return MaterialInfo(
        material_code=material_code,
        material_name=material_name or material_code,
        ready_to_print_yards=roll_loader.get_ready_to_print_yards(material_code),
        unscoured_or_printable_yards=roll_loader.get_scouring_or_printable_yards_unflagged(
            material_code
        ),
        flagged_yards_otherwise_ready_to_print=roll_loader.get_after_scouring_not_printed_flagged_yards(
            material_code
        ),
        flagged_yards=roll_loader.get_not_yet_printed_flagged_yards(material_code),
        calculated_at=roll_loader.calculated_at.strftime("%Y-%m-%d %H:%M:%S"),
    )


# TODO: probably would be best in the future to get the prevoius day that actually had a printing
def get_previous_business_day():
    today = datetime.now()
    if today.weekday() == 0:  # If today is Monday
        return (today - timedelta(days=3)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    elif today.weekday() == 6:  # If today is Sunday
        return (today - timedelta(days=2)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    elif today.weekday() == 5:  # If today is Saturday
        return (today - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    else:  # If today is Tuesday, Wednesday, Thursday or Friday
        return (today - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )


def get_can_place_order_for_request(skus: OrderInfoRequest, roll_loader: RollLoader):
    if roll_loader is None:
        raise Exception("Unable to load material info")
    from res.flows.make.production.queries import get_orders_since_time
    from res.flows.meta.ONE.queries import fetch_style_bms_data, fix_sku

    # let's say if we get below this number of yards, we can't accept new orders
    material_stock_threshold = 50
    material_to_this_order_quantity = {}
    for sku_and_quantity in skus.skus:
        sku = sku_and_quantity.sku
        bms_data = fetch_style_bms_data(sku)
        if bms_data is None:
            raise Exception(f"Could not find BMS data for {sku}")
        materials = [stats["material_code"] for stats in bms_data["nesting_statistics"]]
        for material in materials:
            if material not in material_to_this_order_quantity:
                material_to_this_order_quantity[material] = 0
            material_to_this_order_quantity[material] += sku_and_quantity.quantity
    # in the future we can get fancy about taking the actual times the rolls were updated
    # but for now, let's just choose a lookback time to overcount and be safe
    # all orders approximately should be accounted for if they were placed an hour before
    # optimus ran for the previous business day, which is the previous business day's start
    hour_before_optimus_ran = get_previous_business_day() - timedelta(hours=1)
    # note that we may still be missing some orders that optimus failed to schedule
    orders_may_not_be_accounted_for = get_orders_since_time(hour_before_optimus_ran)
    # TODO: we should eventually pull the combo materials too for orders coming in...
    # the data is crap so though so it's a lot easier to consider it negligable
    # can pull with hasura the same way we do with the main order above but it would be really slow
    # pulling with some postgres query directly would be probably 10-100x faster
    material_totals = {}

    for order in orders_may_not_be_accounted_for:
        sku = order["sku"]
        prev_order_quantity = order["quantity"]
        primary_body_code = fix_sku(sku).split(" ")[0]
        if primary_body_code not in material_totals:
            material_totals[primary_body_code] = 0
        material_totals[primary_body_code] += prev_order_quantity
    order_material_infos: list[OrderMaterialInfo] = []

    for material, quantity in material_to_this_order_quantity.items():
        if material not in roll_loader.material_info_df.index:
            order_material_infos.append(
                OrderMaterialInfo(
                    material_code=material,
                    scouring_or_printable_yards=0,
                    ready_to_print_yards=0,
                    pending_additional_yards=0,
                    order_required_yards=order_required_yards,
                    expected_remaining_yards=0,
                    safe_to_order=False,
                    reason=f"Material {material} not found in roll loader",
                )
            )
        order_required_yards = quantity * 1.5
        remaining_quantity = roll_loader.get_scouring_or_printable_yards_unflagged(
            material
        )
        # we assume 1.5 yards per body as a safe rough estimate
        expected_quantity_ordered = material_totals.get(material, 0) * 1.5
        expected_remaining = (
            remaining_quantity - expected_quantity_ordered - order_required_yards
        )
        if expected_remaining < material_stock_threshold:
            safe_to_order = False
            reason = f"Requiring {order_required_yards} yards for {material} for {sku} would leave {expected_remaining} yards remaining, below threshold of {material_stock_threshold}"
        else:
            safe_to_order = True
            reason = ""
        order_material_infos.append(
            OrderMaterialInfo(
                material_code=material,
                scouring_or_printable_yards=remaining_quantity,
                ready_to_print_yards=roll_loader.get_ready_to_print_yards(material),
                pending_additional_yards=expected_quantity_ordered,
                order_required_yards=order_required_yards,
                expected_remaining_yards=expected_remaining,
                safe_to_order=safe_to_order,
                reason=reason,
            )
        )
    # TODO: in the future we want to add in suggested material swaps here if possible
    return OrderMaterialInfoResponse(
        sku=sku,
        quantity=quantity,
        material_info=order_material_infos,
        safe_to_order=all([info.safe_to_order for info in order_material_infos]),
        reason="All materials are safe to order"
        if all([info.safe_to_order for info in order_material_infos])
        else ". ".join([info.reason for info in order_material_infos]),
    )
