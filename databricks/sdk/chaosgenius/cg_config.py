import json
from logging import Logger
from typing import Optional

import pandas as pd
from pyspark.sql.session import SparkSession


class CGConfig:
    """
    CG Config class.

    Entity type: workspace, cluster, warehouse, job etc
    Entity ID: ID of above
    Include Entity: "yes"/"no"
    Entity Config: JSON {"something": "else"}
    """

    def __init__(self, sparkSession: SparkSession, logger: Logger):
        self.logger = logger
        self.logger.info("Creating customer config table if not exists.")
        self.sparkSession = sparkSession

        try:
            sparkSession.sql(
                """
                CREATE TABLE IF NOT EXISTS chaosgenius.default.chaosgenius_config (
                    entity_type string,
                    entity_id string,
                    include_entity string,
                    entity_config string
                )
            """
            )
        except Exception:
            self.logger.error("Unable to create config table.", exc_info=True)

        # fetch entire config
        self.logger.info("Fetching chaosgenius config.")
        self.df = self.sparkSession.sql(
            "select * from chaosgenius.default.chaosgenius_config"
        ).toPandas()
        if self.df.empty:
            # this is needed to ensure correct columns are there.
            self.df = pd.DataFrame(
                columns=[
                    "entity_type",
                    "entity_id",
                    "include_entity",
                    "entity_config",
                ]
            )
        self.logger.info("Parsing chaosgenius config.")
        # load string json as dict
        self.df["entity_config"] = (
            self.df["entity_config"].replace("", "{}").apply(lambda x: json.loads(x))
        )
        self.logger.info("Chaosgenius config is ready.")

    def get(
        self,
        entity_type: Optional[str] = None,
        entity_ids: Optional[list[str]] = None,
        include_entity: Optional[str] = None,
        entity_config_filter: Optional[dict] = None,
    ) -> pd.DataFrame:
        try:
            df = self.df.copy()
            if entity_type is not None:
                df = df[df["entity_type"] == entity_type]

            if entity_ids is not None:
                df = df[df["entity_id"].isin(entity_ids)]

            if include_entity is not None:
                df = df[df["include_entity"] == include_entity]

            if entity_config_filter is not None:
                df = df[
                    df["entity_config"].apply(
                        lambda x: all(
                            item in x.items() for item in entity_config_filter.items()
                        )
                    )
                ]
            return df
        except Exception:
            self.logger.error("Unable to get config.", exc_info=True)
            return pd.DataFrame(
                columns=["entity_type", "entity_id", "include_entity", "entity_config"]
            )

    def get_ids(
        self,
        entity_type: Optional[str] = None,
        entity_ids: Optional[list[str]] = None,
        include_entity: Optional[str] = None,
        entity_config_filter: Optional[dict] = None,
    ) -> set[str]:
        df = self.get(
            entity_type=entity_type,
            entity_ids=entity_ids,
            include_entity=include_entity,
            entity_config_filter=entity_config_filter,
        )
        if df.empty:
            return set()
        return set(df["entity_id"].values.tolist())
