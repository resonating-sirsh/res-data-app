import res

import res
from res.flows.finance.queries import (
    update_rates,
    update_material_rate,
    get_material_rates,
    insert_material_rate,
    get_sew_labor_rates,
    get_sew_labor_rates_s3,
    update_s3_sew_labor_rate,
)
import pandas as pd
import traceback
from res.connectors.airtable import AirtableConnector
import json


def sync_all_rates(event, context={}, use_kgateway=False, force=None):
    """
    we will cal this in the generic infra daily updates
    """
    try:
        sync_material_rates(use_kgateway=use_kgateway, force=force)
    except:
        res.utils.logger.warn(f"Failed material rates sync {traceback.format_exc()}")
    try:
        sync_node_rates(use_kgateway=use_kgateway)
    except:
        res.utils.logger.warn(f"Failed node rates sync {traceback.format_exc()}")


def sync_material_rates(hasura=None, use_kgateway=False, force=False):
    hasura = hasura or res.connectors.load("hasura")
    airtable = res.connectors.load("airtable")
    kafka = res.connectors.load("kafka")

    res.utils.logger.info("Loading the material rates")
    adata = (
        airtable["appoaCebaYWsdqB30"]["tblJOLE1yur3YhF0K"].to_dataframe(
            filters=AirtableConnector.make_key_lookup_predicate(
                ["Materials"], "Material Taxonomy Categories"
            ),
            fields=[
                "Material Code",
                "Published Resonance Price ($)",
                "Material Name",
                "Fabric Type",
                "Dominant Content",
            ],
        )
        #
    )
    adata = adata.dropna()

    # a map of materials to fabric prices
    luPrice = dict(adata[["Material Code", "Published Resonance Price ($)"]].values)

    # a map of materials to ids
    ids = dict(adata[["Material Code", "record_id"]].values)

    res.utils.logger.info("Comparing records we have...")
    hasura_rates = get_material_rates()
    res.utils.logger.info("Loading sew labor rates from postgres...")
    sew_labor_rates_pg = get_sew_labor_rates()
    # we use S3 copy of sew costs per material as snapshot of the last time we ran, to compare against for changes
    res.utils.logger.info("Loading previous sew labor rates from S3 for comparison...")
    sew_labor_rates_s3 = get_sew_labor_rates_s3()

    existing_in_hasura = list(hasura_rates["material_code"].unique())
    # if its completely new
    for source_record in (
        adata.rename(
            columns={
                "Material Code": "material_code",
                "Material Name": "name",
                "Published Resonance Price ($)": "fabric_price",
                "Dominant Content": "dominant_content",
                "Fabric Type": "fabric_type",
                # "record_id": "airtable_record_id",
            }
        )
        .dropna()
        .drop(["__timestamp__", "record_id"], 1)
        .to_dict("records")
    ):
        if source_record["material_code"] not in existing_in_hasura or source_record[
            "material_code"
        ] in (force or []):
            res.utils.logger.info(f"New material {source_record['material_code']}")
            insert_material_rate(source_record)

            # we need to calc the sew labor rate to publish that along with the new material. Sew labor rate now associated with the material, and is a function of fabric type + dominant content
            sew_labor_rate = calc_sew_labor_rate(
                sew_labor_rates_pg,
                source_record["dominant_content"],
                source_record["fabric_type"],
                source_record["material_code"],
            )

            message = {
                "id": res.utils.res_hash(),
                "created_at": res.utils.dates.utc_now_iso_string(),
                "material_code": source_record["material_code"],
                "units": "yards",
                "rate": source_record["fabric_price"],
                "airtable_record_id": ids.get(source_record["material_code"]),
                # strictly these next two (fabric_type, dominant_content) are not needed, but adding for completeness, as they now live in hasura and feed into sew costs
                "fabric_type": source_record["fabric_type"],
                "dominant_content": source_record["dominant_content"],
                # cost to sew this material, per minute
                "sew_node_rate_minutes": sew_labor_rate,
            }

            res.utils.logger.info(
                f"publishing res_finance.rates.fabric_cost_updates for new material: {json.dumps(message, indent=4)}"
            )
            kafka["res_finance.rates.fabric_cost_updates"].publish(
                message,
                use_kgateway=use_kgateway,
            )

    """
    loop the materials and when the rate of any changes update the database and publish it    
    """
    for record in hasura_rates.to_dict("records"):
        material_code = record["material_code"]
        old_rate = record["fabric_price"]
        mc = luPrice.get(material_code)
        if not mc:
            continue

        # this cost to sew per minute is now a function of fabric type + dominant content, and thus published with the material
        curr_sew_labor_rate = calc_sew_labor_rate(
            sew_labor_rates_pg,
            record["dominant_content"],
            record["fabric_type"],
            material_code,
        )
        old_sew_rate = sew_labor_rates_s3.get(material_code)
        sew_labor_rate_changed = old_sew_rate is None or (
            round(curr_sew_labor_rate, 5) != round(old_sew_rate, 5)
        )
        new_rate = round(mc, 5)

        if (old_rate != new_rate) or (sew_labor_rate_changed):
            res.utils.logger.info(
                f"Material rate changed as {old_rate} != {new_rate} or sew rate as {curr_sew_labor_rate} != {old_sew_rate} for material {material_code}"
            )
            record["fabric_price"] = new_rate
            old_id = record["id"]
            record["id"] = res.utils.uuid_str_from_dict(record)

            # Only updating if rate per yard to buy material has changed. an update on Sew Labor should not cause an update here
            if old_rate != new_rate:
                # add the new record and terminate the old
                update_material_rate(record=record, old_id=old_id)

            message = {
                "id": res.utils.res_hash(),
                "created_at": res.utils.dates.utc_now_iso_string(),
                "material_code": record["material_code"],
                "units": "yards",
                "rate": record["fabric_price"],
                "airtable_record_id": ids.get(record["material_code"]),
                # strictly these next two (fabric_type, dominant_content) are not needed, but adding for completeness, as they now live in hasura and feed into sew costs
                "fabric_type": record["fabric_type"],
                "dominant_content": record["dominant_content"],
                "sew_node_rate_minutes": curr_sew_labor_rate,
            }
            res.utils.logger.info(
                f"publishing res_finance.rates.fabric_cost_updates  - {json.dumps(message, indent=4)}"
            )
            # relay the new record to kafka
            kafka["res_finance.rates.fabric_cost_updates"].publish(
                message,
                use_kgateway=use_kgateway,
            )

            if sew_labor_rate_changed:
                sew_labor_rates_s3[material_code] = float(round(curr_sew_labor_rate, 5))
                update_s3_sew_labor_rate(sew_labor_rates_s3)


def calc_sew_labor_rate(rates: pd.DataFrame, dominant_content, fabric_type, material):
    filteredrates = rates[
        (rates["dominant_content"] == dominant_content)
        & (rates["fabric_type"] == fabric_type)
    ]
    if not filteredrates.empty:
        return float(filteredrates.iloc[0]["cost_per_minute"])
    else:
        filteredrates = rates[
            (rates["fabric_type"] == fabric_type) & (rates["dominant_content"].isnull())
        ]
        if not filteredrates.empty:
            return float(filteredrates.iloc[0]["cost_per_minute"])
        else:
            res.utils.logger.error(
                f"Cant find sew labor rates for {material}, Likely fabric type and / or dom content are None. This is an ERROR. however defaulting to max"
            )
            return float(rates["column_name"].max())


def sync_node_rates(hasura=None, use_kgateway=False):
    hasura = hasura or res.connectors.load("hasura")
    airtable = res.connectors.load("airtable")
    kafka = res.connectors.load("kafka")

    tab = airtable["appziWrNVKw5gpcnW"]["tblSKiDnu9ntXcjUD"].to_dataframe(
        fields=["NodePrice", "Node Name"]
    )
    tab = tab[tab["Node Name"].isin(["Print", "Cut"])].set_index("Node Name")

    current_rates = {
        "cut_node_rate_pieces": tab.loc["Cut"]["NodePrice"],
        "print_node_rate_yards": tab.loc["Print"]["NodePrice"],
        # "sew_node_rate_minutes" is now material specific
    }
    rids = {
        "cut_node_rate_pieces": tab.loc["Cut"]["record_id"],
        "print_node_rate_yards": tab.loc["Print"]["record_id"],
        # "sew_node_rate_minutes" is now material specific
    }

    # check hasura

    Q = """query MyQuery {
          finance_node_rates(where: {ended_at: {_is_null: true}}) {
            id
            cut_node_rate_pieces
            default_overhead
            ended_at
            print_node_rate_yards
            updated_at
            created_at
          }
        }
        """
    fdata = hasura.execute_with_kwargs(Q)["finance_node_rates"]
    if len(fdata) == 0:
        raise Exception(
            "No initial data for the rates - please insert a record with an open / null end date"
        )
    fdata = fdata[0]

    OLD_ID = fdata["id"]
    changed = False
    for c in ["cut_node_rate_pieces", "print_node_rate_yards"]:
        if fdata[c] != current_rates[c]:
            res.utils.logger.info(f"Changed rate {c}: {fdata[c]} != {current_rates[c]}")
            fdata[c] = current_rates[c]
            # generating a new record if anything changes
            fdata["id"] = res.utils.uuid_str_from_dict(fdata)
            fdata["ended_at"] = None
            ####
            changed = True
            u = {
                "id": res.utils.res_hash(),
                "created_at": res.utils.dates.utc_now_iso_string(),
                "node_name": c.split("_")[0],
                "units": c.split("_")[-1],
                "rate": current_rates[c],
                "airtable_record_id": rids[c],
            }

            kafka["res_finance.rates.node_rate_updates"].publish(
                u, use_kgateway=use_kgateway
            )

    if changed:
        res.utils.logger.info(f"Changed rates updates...")
        r = update_rates(fdata, old_id=OLD_ID)
        res.utils.logger.info(r)

    return tab
