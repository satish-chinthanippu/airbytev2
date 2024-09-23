# generated by datamodel-codegen:
#   filename:  ConnectorBuildOptions.yaml

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Extra


class ConnectorBuildOptions(BaseModel):
    class Config:
        extra = Extra.forbid

    baseImage: Optional[str] = None
