from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, PostgresDsn
from typing import Annotated

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ETL_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )
    
    db_host: Annotated[str, Field(default="localhost", min_length=1)]
    db_port: Annotated[int, Field(default=5432, ge=1, le=65535)]
    db_name: Annotated[str, Field(default="etl", min_length=1)]
    db_user: Annotated[str, Field(default="etl", min_length=1)]
    db_password: Annotated[str, Field(default="etl", min_length=1)]

    @property
    def sqlalchemy_url(self) -> PostgresDsn:
        return PostgresDsn.build(
            scheme = "postgresql+psycopg2",
            username = self.db_user,
            password = self.db_password,
            host = self.db_host,
            port = self.db_port,
            path = f"/{self.db_name}"
        )

settings = Settings()
