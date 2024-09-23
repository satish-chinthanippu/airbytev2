# generated by datamodel-codegen:
#   filename:  GeneratedFields.yaml

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Extra, Field


class GitInfo(BaseModel):
    class Config:
        extra = Extra.forbid

    commit_sha: Optional[str] = Field(None, description="The git commit sha of the last commit that modified this file.")
    commit_timestamp: Optional[datetime] = Field(None, description="The git commit timestamp of the last commit that modified this file.")
    commit_author: Optional[str] = Field(None, description="The git commit author of the last commit that modified this file.")
    commit_author_email: Optional[str] = Field(None, description="The git commit author email of the last commit that modified this file.")


class SourceFileInfo(BaseModel):
    metadata_etag: Optional[str] = None
    metadata_file_path: Optional[str] = None
    metadata_bucket_name: Optional[str] = None
    metadata_last_modified: Optional[str] = None
    registry_entry_generated_at: Optional[str] = None


class GeneratedFields(BaseModel):
    git: Optional[GitInfo] = None
    source_file_info: Optional[SourceFileInfo] = None
