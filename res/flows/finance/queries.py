import os
import socket

machine_name = socket.gethostname()
if "Adnans-MacBook-Pro.local" in machine_name:
    os.environ["RES_ENV"] = "production"
    os.environ["RDS_SERVER"] = "localhost:5432"
import res
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
from decimal import Decimal
from typing import Optional
from dataclasses import dataclass


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def insert_material_rate(record, hasura=None):
    """
    take a new record - terminate the old
    """
    hasura = hasura or res.connectors.load("hasura")

    U = """mutation MyMutation( $objects: [finance_material_rates_insert_input!] = {}) {
         
        insert_finance_material_rates(objects: $objects, on_conflict: {constraint: material_rates_pkey,
            update_columns: [name, material_code, fabric_price, dominant_content, fabric_type]}) {
            returning {
            id
            }
        }
        }

    """

    r = hasura.execute_with_kwargs(U, objects=[record])
    return r


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def update_material_rate(record, old_id, hasura=None):
    """
    take a new record - terminate the old
    """
    hasura = hasura or res.connectors.load("hasura")

    U = """mutation MyMutation($ended_at: timestamptz = "", $id: uuid = "", $objects: [finance_material_rates_insert_input!] = {}) {
        update_finance_material_rates(where: {id: {_eq: $id}}, _set: {ended_at: $ended_at}) {
            returning {
            id
            }
        }
        insert_finance_material_rates(objects: $objects, on_conflict: {constraint: material_rates_pkey,
            update_columns: [name, material_code, fabric_price, dominant_content, fabric_type]}) {
            returning {
            id
            }
        }
        }

    """

    r = hasura.execute_with_kwargs(
        U, id=old_id, objects=[record], ended_at=res.utils.dates.utc_now_iso_string()
    )
    return r


def update_s3_sew_labor_rate(rates_dict: dict):
    # s3 store of this dataframe acts as the previous version of sew labor rates, for comparison purposes, on publish kafka message decision
    s3 = res.connectors.load("s3")

    df = pd.DataFrame(
        list(rates_dict.items()), columns=["material_code", "sew_labor_rate"]
    )
    res.utils.logger.info(
        "writing new sew labor rates to s3://res-data-platform/finance-rates-sew-labor/sew-labor-rates.csv"
    )

    s3.write("s3://res-data-platform/finance-rates-sew-labor/sew-labor-rates.csv", df)


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def update_rates(rates, old_id, hasura=None):
    hasura = hasura or res.connectors.load("hasura")

    U = """mutation update_finance_rates($ended_at: timestamptz = "", $id: uuid = "", $rates: [finance_node_rates_insert_input!] = {}) {
          update_finance_node_rates(where: {id: {_eq: $id}}, _set: {ended_at: $ended_at}) {
            returning {
              id
            }
          }
          insert_finance_node_rates(objects: $rates, on_conflict: {constraint: node_rates_pkey, 
            update_columns: [cut_node_rate_pieces, sew_node_rate_minutes,print_node_rate_yards, default_overhead]}) {
            returning {
              id
            }
          }
        }
        """

    r = hasura.execute_with_kwargs(
        U,
        id=old_id,
        rates=[rates],
        ended_at=res.utils.dates.utc_now_iso_string(),
    )
    return r


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def get_material_rates(hasura=None):
    Q = """query get_material_rates {
            finance_material_rates(where: {ended_at: {_is_null: true}}) {
                id
                material_code
                name
                fabric_price
                dominant_content
                fabric_type,
                ended_at
            }
            }
        """

    hasura = hasura or res.connectors.load("hasura")
    data = pd.DataFrame(hasura.execute_with_kwargs(Q)["finance_material_rates"])
    return data


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def get_sew_labor_rates(postgres=None):
    Q = """SELECT cost_per_minute, fabric_type, dominant_content, start_date, end_date 
    FROM finance.material_sew_labor_rates
    WHERE 
    (is_deleted IS NULL OR is_deleted = false)
    AND start_date <= CURRENT_DATE
    AND end_date >= CURRENT_DATE
        """

    postgres = postgres or res.connectors.load("postgres")
    data = postgres.run_query(Q)
    return data


def get_sew_labor_rates_s3():
    s3 = res.connectors.load("s3")
    revtal = dict()
    try:
        df = s3.read(
            "s3://res-data-platform/finance-rates-sew-labor/sew-labor-rates.csv"
        )
        revtal = dict(df[["material_code", "sew_labor_rate"]].values)
    except Exception as ex:
        res.utils.logger.error(
            f"could not read sew laboor rates from s3: {ex}.  Will assume no existing sew labor rates and will republish all materials sew labor rate"
        )
    finally:
        return revtal


def get_sew_labor_cost(material_code: str = None):
    def raise_exception(message):
        res.utils.logger.error(message)
        raise Exception(message)

    query_materials = """
    SELECT materials.fabric_type, materials.dominant_content, materials.material_code
    FROM finance.material_rates as materials
    WHERE materials.material_code = %s
    AND (materials.ended_at::timestamp >= CURRENT_TIMESTAMP OR materials.ended_at is null)
    ORDER by updated_at DESC;
    """

    query_rates = """
    SELECT rates.cost_per_minute, rates.fabric_type, rates.dominant_content
    FROM finance.material_sew_labor_rates as rates
    WHERE rates.start_date <= CURRENT_DATE
    AND rates.end_date >= CURRENT_DATE
    AND rates.is_deleted is not true;
    """

    @dataclass
    class SewLaborRates:
        fabric_type: str
        dominant_content: Optional[str]
        cost_per_minute: Decimal

    @dataclass
    class SewLaborMaterials:
        fabric_type: str
        dominant_content: Optional[str]
        material_code: str

    postgres = res.connectors.load("postgres")
    params = []
    params.append(str(material_code))
    df_materials = postgres.run_query(query_materials, params, keep_conn_open=True)
    df_rates = postgres.run_query(query_rates)

    rates = [SewLaborRates(**row) for _, row in df_rates.iterrows()]
    if not rates:
        raise_exception(f"No rates found for {material_code}")
    materials = [SewLaborMaterials(**row) for _, row in df_materials.iterrows()]
    if len(materials) == 0:
        raise_exception(f"No materials rates: {material_code}.")

    material = materials[0]
    if not (material.fabric_type):
        raise_exception(f"Null fabric_type on material code: {material_code}.")

    for rate in rates:
        if (
            rate.dominant_content == material.dominant_content
            and rate.fabric_type == material.fabric_type
        ):
            return rate

    for rate in rates:
        if rate.fabric_type == material.fabric_type and not rate.dominant_content:
            return rate

    raise_exception(f"Sew labor rates not found for code: {material_code}.")


def get_node_rates(expand=True, hasura=None):
    Q = """query get_node_rates {
    finance_node_rates(where: {ended_at: {_is_null: true}}) {
        updated_at
        ended_at
        sew_node_rate_minutes
        print_node_rate_yards
        id
        default_overhead
        cut_node_rate_pieces
        created_at
    }
    }
    """

    hasura = hasura or res.connectors.load("hasura")
    r = hasura.execute_with_kwargs(Q)["finance_node_rates"]
    if expand:
        r = r[0]

        df = pd.DataFrame(
            [
                {
                    "item": "Overhead",
                    "category": "Overhead",
                    "rate": r.get("default_overhead"),
                    "unit": "",
                },
                {
                    "item": "Print",
                    "rate": r.get("print_node_rate_yards"),
                    "unit": "Yards",
                },
                {
                    "item": "Cut",
                    "rate": r.get("cut_node_rate_pieces"),
                    "unit": "Pieces",
                },
                {
                    "item": "Sew",
                    "rate": r.get("sew_node_rate_minutes"),
                    "unit": "Minutes",
                },
            ]
        )
        df["category"] = df["category"].fillna("Labor")
        return df

    return r


def get_material_rates_by_material_codes(material_codes, hasura=None):
    if isinstance(material_codes, str):
        material_codes = [material_codes]
    Q = """query get_rates_for_material_codes($material_codes: [String!] = "") {
    finance_material_rates(where: {material_code: {_in: $material_codes}}) {
        fabric_price
        material_code
        name
        updated_at
        ended_at
    }
    }
    """

    hasura = hasura or res.connectors.load("hasura")

    df = pd.DataFrame(
        hasura.execute_with_kwargs(Q, material_codes=material_codes)[
            "finance_material_rates"
        ]
    )

    # implement interface
    df["item"] = df.apply(lambda row: f"{row['name']} - {row['material_code']}", axis=1)
    df["unit"] = "Yards"
    df["category"] = "Materials"
    df["rate"] = df["fabric_price"]
    df = df[df["ended_at"].isnull()].reset_index(drop=True)

    return df
