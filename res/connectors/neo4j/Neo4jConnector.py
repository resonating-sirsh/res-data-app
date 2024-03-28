from .. import DatabaseConnector, DatabaseConnectorTable, DatabaseConnectorSchema
import pandas as pd
import neo4j
from pydantic import BaseModel, root_validator
import res
import typing


class NeoEdge(BaseModel):
    configuration: str = "default"
    ##fio by directionally verbs we can add multiple links e.g. belongs-to or contained-in could be two directions of the same edge
    directionality_verb: str = "links"
    weight: int = 0


class NeoNode(BaseModel):
    code: str
    # only because we default to code
    name: typing.Optional[str] = ""
    namespace: str = "default"

    def upsert_predicate(cls, node_tag="n"):
        return f"(a: {cls.__node_type}{{ $code:{cls.code} }}"

    @property
    def node_type(cls):
        # temp ref replace
        """
        the nde name is the class name except we have a Ref convention
        """
        a = cls.schema()["title"]
        if a[-3:] == "Ref":
            return a[:-3]
        return a

    @root_validator
    def _ids(cls, values):
        if not values.get("name"):
            values["name"] = values.get("code")
        return values


class Neo4jConnector(DatabaseConnector):
    def __init__(self):
        # Connect to the Neo4j database
        self.uri = "bolt://localhost:7687"  # Update with your Neo4j URI
        self.user = "neo4j"  # Update with your Neo4j username
        self.password = "password"  # Update with your Neo4j password
        self.driver = neo4j.GraphDatabase.driver(
            self.uri, auth=(self.user, self.password)
        )

    def get_schema(self):
        # call db.schema.visualization()
        pass

    def _delete_all(self, tx):
        return tx.run("MATCH (n) DETACH DELETE n")

    def delete_all(self):
        return self.run(self._delete_all)

    def do_llm_query(self, question):
        pass

    def _delete_nodes(self, tx, node_ids):
        """
        find a better expression -> both with and without relationships
        """
        for node_id in node_ids:
            a = tx.run(
                "MATCH (n)-[r]-() WHERE id(n) = $node_id DELETE n, r", node_id=node_id
            )

        for node_id in node_ids:
            tx.run("MATCH (n) WHERE id(n) = $node_id DELETE n", node_id=node_id)

    def _add_record(self, tx, record, key="code"):
        """ """
        d = record.dict()
        obj_type = record.schema()["title"]
        # for insert we would just insert everything but for upser we match the key
        # params = ", ".join([f'{a}: ${a}' for a in d.keys()])
        params = ", ".join([f"{a}: ${a}" for a in [key]])
        exp = f"MERGE (n:{obj_type} {{{key}: $key_value}}) SET n += $new_properties"
        props = {k: v for k, v in d.items() if k != key}
        res.utils.logger.debug(f"{exp=}, {props=}")
        return tx.run(exp, key_value=d[key], new_properties=props)

    def _add_records(self, tx, records):
        if not isinstance(records, list):
            records = [records]
        for r in records:
            self._add_record(tx, record=r)

    def add_relationship(self, a, b, rel):
        attributes = rel.dict()
        rel_type = rel.schema()["title"]
        return self.run(
            self._add_relationship_with_attributes,
            a=a,
            b=b,
            relationship_type=rel_type,
            attributes=attributes,
        )

    def _add_relationship_with_attributes(
        self, tx, a, b, relationship_type, attributes
    ):
        exp = f"MATCH (a:{a.node_type}), (b:{b.node_type}) WHERE a.code = $key_value_1 AND b.code = $key_value_2  MERGE (a)-[r:{relationship_type} {{configuration: $configuration}}]->(b)"
        res.utils.logger.debug(f"{exp=}, {attributes=}")

        return tx.run(exp, key_value_1=a.code, key_value_2=b.code, **attributes)

    def add_records(self, records):
        return self.run(self._add_records, records=records)

    def run(self, op, **kwargs):
        with self.driver.session() as session:
            return session.write_transaction(op, **kwargs)

    def delete_nodes(self, ids):
        return self.run(self._delete_nodes, node_ids=ids)

    def close(self):
        self.driver.close()
