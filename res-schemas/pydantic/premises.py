from typing import List, Optional
from schemas.pydantic.common import (
    FlowApiModel,
    Field,
    root_validator,
    uuid_str_from_dict,
)


class RfidEventRequest(FlowApiModel):
    id: Optional[str] = Field(key_field=True, primary_key=True)
    reader: str
    channel: str
    tag_epc: str
    observed_at: str
    metadata: Optional[dict]

    @root_validator
    def _ids(cls, values):
        values["id"] = uuid_str_from_dict(
            {
                "reader": values["reader"],
                "channel": values["channel"],
                "tag_epc": values["tag_epc"],
                "observed_at": values["observed_at"],
            }
        )
        return values


class RfidEventResponse(FlowApiModel):
    id: str = Field(key_field=True, primary_key=True)
    events_id: Optional[str]
    event: str
    observed_at: str
    observed_item: Optional[dict]
    metadata: Optional[dict]

    @root_validator
    def _ids(cls, values):
        key = list(values["observed_item"].keys())[0]
        values["events_id"] = uuid_str_from_dict(
            {
                "observed_item": values["observed_item"][key],
                "metadata": values["metadata"]["rfid_tags"],
            }
        )
        return values
