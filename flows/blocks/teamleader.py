from prefect.blocks.core import Block
from pydantic.v1 import Field, SecretStr


class TeamleaderCredentials(Block): # type: ignore
    """
    Block for storing Teamleader credentials.
    """

    client_id: str = Field(
        default="",
        description="The client ID for the Teamleader application.",
    )
    client_secret: SecretStr = Field(
        default=SecretStr(""),
        description="The client secret for the Teamleader application.",
    )

    class Config:
        """Block configuration."""

        name = "Teamleader Credentials"
        version = "1.0.0"