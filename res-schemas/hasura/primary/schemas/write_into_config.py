from __future__ import annotations
from itertools import chain, starmap
from pathlib import Path
import re
import os
import sys
from typing import Any, Dict, List, Optional, TypedDict, Union
import yaml

HASURA_DIR = f"{os.environ.get('PROJECT_ROOT')}/res-schemas/hasura/primary"
PATH_CONFIG = f"{HASURA_DIR}/metadata/remote_schemas.yaml"
SCHEMA_FOLDER_NAME = "schemas"
SCHEMA_FOLDER = f"{HASURA_DIR}/{SCHEMA_FOLDER_NAME}"
REMOTE_SCHEMA_NAME = "create-one-api"

ADD_NEW_SCHEMAS = os.getenv("ADD_NEW_SCHEMAS", "f").lower()[0] not in ["f", "0"]
ADD_NEW_PERMISSIONS = os.getenv("ADD_NEW_PERMISSIONS", "f").lower()[0] not in ["f", "0"]


class YAMLLiteral(str):
    pass


class Header(TypedDict):
    name: str
    value: str


class PermissionDefinition(TypedDict):
    schema: YAMLLiteral


class Permission(TypedDict):
    role: str
    definition: PermissionDefinition


class RemoteSchema(TypedDict):
    comment: str
    name: str
    definition: Dict[str, Any]
    permissions: List[Permission]


def literal_representer(dumper: yaml.Dumper, data: YAMLLiteral):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


yaml.add_multi_representer(YAMLLiteral, literal_representer)


def get_remote_schema_folder(remote_schema: Union[str, Path]) -> Path:
    if isinstance(remote_schema, str):
        if Path(remote_schema).exists():
            remote_schema = Path(remote_schema)
        else:
            remote_schema = Path(SCHEMA_FOLDER) / remote_schema
    if not remote_schema.exists() or not remote_schema.is_dir():
        raise ValueError(
            f"Error: Can't find folder for remote schema '{remote_schema.name}'!"
        )
    if not remote_schema.parent.samefile(SCHEMA_FOLDER):
        raise ValueError(
            f"Error: '{remote_schema!s}' is not in schema folder: '{SCHEMA_FOLDER}'!"
        )
    return remote_schema


def get_permission_file(
    file: Union[str, Path], *, remote_schema: Optional[Union[str, Path]] = None
) -> Path:
    if isinstance(file, str):
        file = Path(file)
    if remote_schema is None:
        if not file.parent:
            raise ValueError(
                f"Error: Not sure what to do with file '{file!r}' and no schema!"
            )
        remote_schema = file.parent.name
        if not file.exists():
            file = Path(file.name)
    remote_schema = get_remote_schema_folder(remote_schema)
    for part in reversed(remote_schema.parts):
        if file.exists():
            break
        file = Path(part) / file
    if not file.exists() or not file.is_file():
        raise ValueError(
            f"Error: Can't find permission schema for role '{file.stem}' in remote "
            f"schema {remote_schema.name}!"
        )
    if not file.parent.samefile(remote_schema.absolute()):
        raise ValueError(
            f"Error: Permission schema file '{file!s}' is not in the folder of remote "
            f"schema '{remote_schema.name}'!"
        )
    return file


def get_permission_from_file(
    file: Union[str, Path], *, remote_schema: Optional[Union[str, Path]] = None
):
    file = get_permission_file(file, remote_schema=remote_schema).absolute()
    role = re.sub(r"\W", "_", file.name.split(".")[0].lower())
    schema = file.read_text(encoding="utf-8").strip("\n")
    if schema:
        yield role, Permission(role=role, definition={"schema": YAMLLiteral(schema)})
    else:
        print(
            f"Warning: Found empty permission schema for role '{role}' in remote "
            f"schema '{file.parent.name}'!"
        )


def get_all_permissions_for_schema(
    remote_schema: Union[str, Path] = REMOTE_SCHEMA_NAME
):
    remote_schema = get_remote_schema_folder(remote_schema)
    for file in remote_schema.iterdir():
        if file.is_file():
            yield from get_permission_from_file(file, remote_schema=remote_schema)


def get_permissions_from_source(source: Union[str, Path] = REMOTE_SCHEMA_NAME):
    errs = []
    try:
        remote_schema = get_remote_schema_folder(source)
        updates = dict(get_all_permissions_for_schema(remote_schema))
        if updates:
            yield remote_schema, remote_schema.name, updates
        return
    except ValueError as exc:
        errs.append(str(exc))
    try:
        permission_file = get_permission_file(source)
        updates = dict(get_permission_from_file(permission_file))
        if updates:
            yield permission_file, permission_file.parent.name, updates
        return
    except ValueError as exc:
        errs.append(str(exc))
    raise ValueError(f"Not sure what to do with '{source!s}'! ({errs!r})")


def get_updates_from_sources(*sources: Union[str, Path]):
    if not sources:
        print(f"No sources provided! Using default source '{REMOTE_SCHEMA_NAME}'.")
        sources = (REMOTE_SCHEMA_NAME,)
    schema_updates: Dict[str, Dict[str, Permission]] = {}
    for source, schema_name, permissions_dict in chain.from_iterable(
        map(get_permissions_from_source, sources)
    ):
        print(f"Source '{source!s}' provided update to schema '{schema_name}'.")
        schema_updates[schema_name] = {
            **(schema_updates.get(schema_name, {})),
            **permissions_dict,
        }
    return schema_updates


def make_new_schema_for_permissions(name: str, permissions: Dict[str, Permission]):
    print(
        f"Warning: Adding new remote schema '{name}' with sensible defaults, please "
        f"double-check the file after ({PATH_CONFIG})!"
    )
    url_from_env = re.sub(r"\W", "_", name).upper()
    return RemoteSchema(
        name=name,
        comment="",
        definition=dict(url_from_env=url_from_env, timeout_seconds=60),
        permissions=list(permissions.values()),
    )


def write_permissions_on_rule(
    *sources: str,
    remote_schema: Optional[Union[str, Path]] = None,
    add_new_schemas: bool = ADD_NEW_SCHEMAS,
    add_new_permissions: bool = ADD_NEW_PERMISSIONS,
):
    schema_updates = get_updates_from_sources(*sources)
    if remote_schema is not None:
        remote_schema = get_remote_schema_folder(remote_schema)
        schema_updates = {
            schema_name: permissions_dict
            for schema_name, permissions_dict in schema_updates.items()
            if schema_name == remote_schema.name
        }
    if not schema_updates:
        print("Nothing to do!")
        return
    file_contents = Path(PATH_CONFIG).read_text(encoding="utf-8")
    backup = Path(PATH_CONFIG).with_suffix(".graphql.backup")
    backup.write_text(file_contents, encoding="utf-8")
    remote_schemas: List[RemoteSchema] = yaml.full_load(file_contents)

    # finding right config section
    for schema in remote_schemas:
        permission_updates = schema_updates.pop(schema["name"], {})
        if not permission_updates:
            print(f"No updates for remote schema '{schema['name']}'.")

        # validate if the role exists
        for permission in schema["permissions"]:
            role = permission["role"]
            role_update = permission_updates.pop(role, None)
            if role_update and role_update != permission:
                print(f"Updated role '{role}' in schema '{schema['name']}'.")
                permission.update(role_update)
            else:
                message = "No updates" if not role_update else "Nothing to do"
                print(f"{message} for role '{role}' in schema '{schema['name']}'.")
                permission["definition"]["schema"] = YAMLLiteral(
                    permission["definition"]["schema"]
                )

        # handle new permissions
        if add_new_permissions:
            for role, role_update in permission_updates.items():
                schema["permissions"].append(role_update)
                print(f"Added new role '{role}' to schema '{schema['name']}'.")
        elif permission_updates:
            print(
                f"Warning: Unhandled new roles {tuple(permission_updates.keys())!s} "
                f"found for schema '{schema['name']}'!"
            )

    # handle new schemas
    if add_new_schemas:
        remote_schemas.extend(
            starmap(make_new_schema_for_permissions, schema_updates.items())
        )
    elif schema_updates:
        print(f"Warning: Unhandled new schemas {schema_updates.keys()} found!")

    yaml.dump(remote_schemas, Path(PATH_CONFIG).open("w", encoding="utf-8"))
    # print(final_config)

    print(f"Config File updated on {PATH_CONFIG}")


if __name__ == "__main__":
    write_permissions_on_rule(*sys.argv[1:])
