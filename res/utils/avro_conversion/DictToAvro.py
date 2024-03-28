import os, json, xmltodict, sys
from genson import SchemaBuilder


# This class will accept raw JSON payload data (in the form of a python dict) and generate an Avro
# schema for use with the Kafka Schema Registry. It will do nested data, including objects and arrays.
# TODO: may need to add support for other data types, and for an array of primitives or array of arrays


class DictToAvro:
    def __init__(self):
        self.json_to_avro_field_types = {"integer": "int", "number": "float"}

    def get_avro(
        self,
        json_schema_properties,
        namespace,
        objectName="topLevelItem",
    ):
        this_namespace = ".".join(namespace.split(".")[:-1])
        errors = []
        return_schemas = []
        main_schema = {
            "type": "record",
            "name": objectName,
            "namespace": this_namespace,
            "fields": [],
        }
        for property in json_schema_properties:
            temp_type = json_schema_properties[property]["type"]
            if temp_type == "object":
                # Add nested schemas to front of schemas
                temp_schemas, temp_errors = self.get_avro(
                    json_schema_properties[property]["properties"],
                    f"{namespace}.{property}",
                    property,
                )
                errors += temp_errors
                return_schemas = temp_schemas + return_schemas
                main_schema["fields"].append(
                    {"name": property, "type": f"{namespace}.{property}"}
                )
            elif temp_type == "array":
                if "items" not in json_schema_properties[property]:
                    # This array is empty, create a string array placeholder until we see data for this item
                    main_schema["fields"].append(
                        {"name": property, "type": {"type": "array", "items": "string"}}
                    )
                elif json_schema_properties[property]["items"]["type"] == "string":
                    main_schema["fields"].append(
                        {"name": property, "type": {"type": "array", "items": "string"}}
                    )
                elif json_schema_properties[property]["items"]["type"] == "object":
                    # Add nested array schemas to front of schemas
                    temp_schemas, temp_errors = self.get_avro(
                        json_schema_properties[property]["items"]["properties"],
                        f"{namespace}.{property}",
                        property,
                    )
                    errors += temp_errors
                    return_schemas = temp_schemas + return_schemas
                    main_schema["fields"].append(
                        {
                            "name": property,
                            "type": {
                                "type": "array",
                                "items": f"{namespace}.{property}",
                            },
                        }
                    )
                else:
                    msg = "Unsupported array type: {}".format(
                        json_schema_properties[property]["items"]["type"]
                    )
                    errors.append(msg)
            elif temp_type in [
                "string",
                "integer",
                "float",
                "boolean",
                "number",
            ]:
                type = self.json_to_avro_field_types.get(
                    temp_type,
                    temp_type,
                )
                main_schema["fields"].append(
                    {
                        "name": property,
                        "type": [type, "null"],
                    }
                )
            elif isinstance(temp_type, list) and len(temp_type) == 2:
                # This is a compound type, generally string,null or int,null
                extracted_type = (
                    temp_type[0] if temp_type[1] == "null" else temp_type[1]
                )
                type = self.json_to_avro_field_types.get(
                    extracted_type,
                    extracted_type,
                )
                main_schema["fields"].append(
                    {
                        "name": property,
                        "type": [type, "null"],
                    }
                )
            elif temp_type in [
                "null",
            ]:
                main_schema["fields"].append(
                    {
                        "name": property,
                        "type": ["string", "null"],
                    }
                )
            else:
                msg = "Unsupported type! {}".format(temp_type)
                errors.append(msg)
        return_schemas.append(main_schema)
        return return_schemas, errors

    # Accepts 1 or more dicts, merges into a JSON schema, then converts to avro
    def dict_to_avro(self, dicts, top_level_namespace):
        builder = SchemaBuilder()
        # First generate JSON schema from dict
        for dict in dicts:
            builder.add_object(dict)
        schema = builder.to_schema()

        # Next generate avro schema from JSON schema
        avro_schema, errors = self.get_avro(schema["properties"], top_level_namespace)
        return avro_schema, errors


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(
            "Enter the filename of a JSON newline-delimited file as the first argument, and a namespace as the second argument"
        )
    else:
        dicts = []
        with open(sys.argv[1]) as json_file:
            for row in json_file:
                dicts.append(json.loads(row))
        dtoA = DictToAvro()
        avro_schema, errors = dtoA.dict_to_avro(dicts, sys.argv[2])
        with open("avro_conversion_output.avsc", "w") as f:
            f.write(json.dumps(avro_schema, indent=2))
        print("errors: {}".format(json.dumps(errors, indent=2)))
