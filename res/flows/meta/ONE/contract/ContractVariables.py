import res
from stringcase import titlecase, snakecase
from schemas.pydantic.meta import ContractVariable
import pandas as pd


class ContractVariables:
    DUCK_PATH = "s3://res-data-platform/data/cache_contract_variables_{0}.parquet"

    def __init__(self, base_id):
        """
        i think sync tables hae ids per base
        """
        self._base_id = base_id
        self._db = res.connectors.load("duckdb")
        self._cache = res.connectors.load("redis")["CACHE"][
            f"CONTRACT_VARIABLES_{base_id}"
        ]

    @staticmethod
    def as_codes(
        contract_variable_names,
        uri="s3://res-data-platform/data/cache_contract_variables_appa7Sw0ML47cA8D1.parquet",
    ):
        """
        Load the contract variables from the s3 cache for the choice body base
        """
        contract_variable_names = [c.rstrip().lstrip() for c in contract_variable_names]
        duck = res.connectors.load("duckdb")
        data = duck.query_from_root(uri).select(fields=["name", "code"]).dropna()
        data["name"] = data["name"].map(lambda x: x.strip())
        data = data[data["name"].map(lambda x: x in contract_variable_names)]
        return list(data["code"].unique()) if len(data) else []

    @staticmethod
    def resolve_node_from_contracts(contract_variables):
        """
        the flow order here is used to resolve

        https://airtable.com/appa7Sw0ML47cA8D1/tblTGrqxE6jkSSDDb/viweXU92pcJMRzzix?blocks=hide

        This could be a lot more efficient but we should find a more systematic way to sync contracts from the airtable to another place
        here we are loading the contracts and then check the ordinal for where they live

        """
        # cv = ContractVariables()
        # cvs = [cv.get(c) for c in contract_variables]
        # cvs = [c for c in cvs if c is not None]
        # cvs = sorted(cvs, key=lambda x: x.ordinal)

        # return cvs
        RESPONSE_NODE = (
            "Validate Body Meta ONE Response"  # previously Validate Meta.ONE Responses
        )
        data = ContractVariables.load("appa7Sw0ML47cA8D1")
        x = data[data["ordinal"].notnull()][["code", "dxa_node", "ordinal"]]
        x["ordinal"] = x["ordinal"].fillna(0)
        x["code"] = x["code"].map(lambda x: x.strip() if x else None)
        x = x.dropna().sort_values("ordinal")
        matches = x[x["code"].isin(contract_variables)]
        match_failed_nodes = list(matches["dxa_node"].unique())
        """
        match the meta one nodes ordinal - some back compat shinnags
        """
        meta_one_node = x[
            x["dxa_node"].isin([RESPONSE_NODE, "Validate Meta.ONE Responses"])
        ].iloc[0]["ordinal"]

        # meta_one_node.replace("Validate Meta.ONE Responses", RESPONSE_NODE)
        x["dxa_node"] = x["dxa_node"].replace(
            "Validate Meta.ONE Responses", RESPONSE_NODE
        )
        """
        select all nodes between the first one that failed contracts and the meta one node
        """
        v = matches.sort_values("ordinal")
        if len(v):
            node_min_ordinal = v.iloc[0]["ordinal"]
            window = (
                x[(x["ordinal"] >= node_min_ordinal) & (x["ordinal"] < meta_one_node)]
                .drop_duplicates(subset=["dxa_node"])
                .sort_values("ordinal")
            )
            window_nodes = list(window["dxa_node"])

            # we select all the nodes between the min contract failing one and where we are and recheck
            # the ones failing contracts are set to Failed
            node_states = {
                f"{node}_Status": "Failed" if node in match_failed_nodes else "Recheck"
                for node in window_nodes
            }

            if len(v):
                return v.iloc[0]["dxa_node"], node_states

        res.utils.logger.warn(f"Failed to match any contracts in {contract_variables}")
        return None, {}

    @staticmethod
    def load_by_codes(contract_variables_codes, base_id, table_id):
        # parse contract variable codes to ids
        cv_formula = (
            "OR(" + ",".join([f"Code='{c}'" for c in contract_variables_codes]) + ")"
            if len(contract_variables_codes) > 0
            else None
        )
        from res.connectors.airtable.AirtableClient import ResAirtableClient

        airtable_client = ResAirtableClient()
        contracts_vars = list(
            airtable_client.get_records(
                base_id,
                table_id,
                filter_by_formula=cv_formula,
                fields=["Variable Name", "Code"],
            )
        )
        return contracts_vars

    @staticmethod
    def load(base_id):
        airtable = res.connectors.load("airtable")
        table_id = airtable[base_id].get_table_id_from_name("Contract Variables")
        res.utils.logger.info(
            f"Loading contracts from source.. in {base_id}.{table_id}"
        )
        flow_table = airtable["appa7Sw0ML47cA8D1"]["tblTGrqxE6jkSSDDb"].to_dataframe(
            fields=["State Name", "Flow Order", "Flow Version"]
        )
        flow_table = (
            flow_table.sort_values("Flow Version")
            .sort_values("Flow Version")
            .drop_duplicates(subset=["State Name"])
        )

        data = res.connectors.load("airtable").get_table_data(f"{base_id}/{table_id}")
        data.columns = [
            snakecase(d.lower()).replace("__", "_").replace("/", "_")
            for d in data.columns
        ]

        flow_table["State Name"] = flow_table["State Name"].map(
            lambda x: x.replace(",", "")
        )
        # drop the flow_table record id, otherwise we get record_id_x and record_id_y
        flow_table = flow_table.drop(columns=["record_id"])

        data = pd.merge(
            data, flow_table, left_on=["dxa_node"], right_on=["State Name"], how="left"
        ).rename(columns={"Flow Order": "ordinal"})

        res.utils.logger.info(
            f"Writing {(len(data))} contracts to {ContractVariables.DUCK_PATH.format(base_id)}"
        )
        data = res.utils.dataframes.replace_nan_with_none(data)

        res.connectors.load("s3").write(
            ContractVariables.DUCK_PATH.format(base_id), data
        )

        return data

    def _try_find(self, query, key):
        cv = self._cache[key]
        if cv:
            return ContractVariable(**cv)

        # here we try once to reload on failure if the table does not exist
        try:
            record = self._db.execute(query)
        except:
            ContractVariables.load(self._base_id)
            record = self._db.execute(query)

        if len(record) == 0:
            ContractVariables.load(self._base_id)

        record = self._db.execute(query)
        if len(record) == 0:
            raise Exception(f"Missing contract [{key}]")
        # assert len(record) == 1, f"Should be only one contract variable for {key}"
        # allow for now and take latest
        cv = ContractVariable(**dict(record.iloc[-1]))
        self._cache[key] = cv.dict()
        return cv

    def try_load_one(self, key):
        # KEY HAS to be like this
        key = key.split("|")[0].strip()
        key = key.upper().replace(" ", "_").replace("__", "").strip()
        query = f""" SELECT * FROM '{ContractVariables.DUCK_PATH.format(self._base_id)}' WHERE code = '{key}' """

        return self._try_find(query, key)

    def try_load_one_by_name(self, key):
        query = f""" SELECT * FROM '{ContractVariables.DUCK_PATH.format(self._base_id)}' WHERE variable_name = '{key}' """

        return self._try_find(query, key)

    def __getitem__(self, key):
        return self.try_load_one(key)

    def try_get(self, key):
        try:
            return self[key]
        except:
            raise
            res.utils.logger.warn(f"Failing to load contract {key}")
        return None
