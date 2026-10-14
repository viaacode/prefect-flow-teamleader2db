from datetime import datetime
from enum import Enum
from typing import Any, Optional, Union

from pydantic import BaseModel, Field, field_serializer
from typing_extensions import TypedDict


class Resource(Enum):
    """
    A Teamleader resource as defined in their [developer documentation](https://developer.teamleader.eu/).
    Names should be copied exactly.
    """

    companies = "companies"
    contacts = "contacts"
    invoices = "invoices"
    departments = "departments"
    events = "events"
    projects = "projects"
    users = "users"
    customFieldDefinitions = "customFieldDefinitions"

    @classmethod
    def get_db_table_name(cls, resource: "Resource") -> str:
        """
        The default database table of the Teamleader resource.
        """
        return DEFAULT_TABLE_NAMES[resource.name]


class ResourceTableNames(TypedDict):
    """
    The database table of each Teamleader resource. Keys must match the Resource names.
    """

    companies: str
    contacts: str
    invoices: str
    departments: str
    events: str
    projects: str
    users: str
    customFieldDefinitions: str


DEFAULT_TABLE_NAMES = ResourceTableNames(
    companies="tl_companies",
    contacts="tl_contacts",
    invoices="tl_invoices",
    departments="tl_departments",
    events="tl_events",
    projects="tl_projects",
    users="tl_users",
    customFieldDefinitions="tl_custom_fields",
)


class TeamleaderRequestInfo(BaseModel):
    """
    A Teamleader request for info on a given resource and id
    """

    base_uri: str = Field(default="https://api.focus.teamleader.eu")
    resource: Resource
    id: str

    @property
    def path(self):
        return self.base_uri + "/" + self.resource.name + ".info"


class TeamleaderRequestList(BaseModel):
    """
    A Teamleader request for a list of resources.
    """

    base_uri: str = Field(default="https://api.focus.teamleader.eu")
    resource: Resource
    page: int = Field(default=1, serialization_alias="page[number]")
    page_size: int = Field(default=20, serialization_alias="page[size]")
    updated_since: Optional[datetime] = Field(
        default=None,
        serialization_alias="filter[updated_since]",
    )

    # needs to be isormat without microsecond ex: '2021-03-29T16:44:33+00:00'
    @field_serializer("updated_since")
    def serialize_updated_since(self, updated_since: datetime, _info):
        _ = _info
        return updated_since.replace(microsecond=0).isoformat()

    @property
    def path(self):
        return self.base_uri + "/" + self.resource.name + ".list"


class TeamleaderResponse(BaseModel):
    resource: Resource
    ratelimit_remaining: int
    ratelimit_reset: datetime
    data: Union[list[dict[str, Any]], dict[str, Any]]


class TeamleaderResponseList(BaseModel):
    resource: Resource
    ratelimit_remaining: int
    ratelimit_reset: datetime
    data: list[dict[str, Any]]


class TeamleaderResponseInfo(BaseModel):
    resource: Resource
    ratelimit_remaining: int
    ratelimit_reset: datetime
    data: dict[str, Any]


def serialize(req) -> dict[str, Any]:
    """
    Convert the Pydantic request models into a Python dictionary.
    """
    if type(req) is TeamleaderRequestList:
        return req.model_dump(
            exclude_none=True,
            by_alias=True,
            include={"page", "page_size", "updated_since"},
        )
    elif type(req) is TeamleaderRequestInfo:
        return {"id": req.id}
    else:
        raise TypeError(
            f"Type of request should be {TeamleaderRequestInfo.__name__} or {TeamleaderRequestList.__name__}"
        )
