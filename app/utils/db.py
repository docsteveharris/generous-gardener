"""
Utilities for connecting to MS SQL server and CosmosDB on Azure
The former is used for the Feature Store, and the latter for storing application state
"""
#  Copyright (c) University College London Hospitals NHS Foundation Trust
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import logging
import os
import struct
from typing import Any

import sqlalchemy
from azure.cosmos import CosmosClient
from azure.identity import DefaultAzureCredential


def is_local():
    return os.environ.get("ENVIRONMENT") is None


def db_aad_token_struct() -> bytes:
    """
    Uses AzureCli login state to get a token for the database scope

    Kindly leveraged from this SO answer: https://stackoverflow.com/a/67692382
    """
    from azure.identity import DefaultAzureCredential

    credential = DefaultAzureCredential()
    token = credential.get_token("https://database.windows.net/")[0]

    token_bytes = bytes(token, "UTF-16 LE")

    return struct.pack("=i", len(token_bytes)) + token_bytes


def odbc_cursor(return_connection: bool = False) -> Any:
    """
    ODBC cursor for running queries against the MSSQL feature store.

    Documentation: https://github.com/mkleehammer/pyodbc/wiki
    """
    import pyodbc

    connection_string = os.environ["FEATURE_STORE_CONNECTION_STRING"]

    if "authentication" not in connection_string.lower():
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        attrs_before = {SQL_COPT_SS_ACCESS_TOKEN: db_aad_token_struct()}
    else:
        attrs_before = {}

    connection = pyodbc.connect(connection_string, attrs_before=attrs_before)
    if return_connection:
        logging.info("ODBC connection created and returned")
        return connection
    cursor = connection.cursor()
    logging.info("ODBC connection cursor created and returned")
    return cursor


def cosmos_client() -> CosmosClient:
    """
    CosmosDB client for connecting with the state store.

    Documentation: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/sdk-python
    """

    environment = os.environ.get("ENVIRONMENT", default="dev")

    client = CosmosClient(
        os.environ["COSMOSDB_ENDPOINT"],
        credential=(
            DefaultAzureCredential()
            if environment != "local"
            else os.environ["COSMOSDB_KEY"]
        ),
        connection_verify=(environment != "local"),
    )
    logging.info("Cosmos client created.")
    return client


def get_sqlalchemy_engine() -> sqlalchemy.engine:
    """
    SQLAlchemy engine for running queries against the MSSQL feature store.
    """
    connection_string = os.environ["FEATURE_STORE_CONNECTION_STRING"]

    if "authentication" not in connection_string.lower():
        SQL_COPT_SS_ACCESS_TOKEN = 1256
        attrs_before = {SQL_COPT_SS_ACCESS_TOKEN: db_aad_token_struct()}
    else:
        attrs_before = {}

    return sqlalchemy.create_engine(
        f"mssql+pyodbc:///?odbc_connect={connection_string}",
        connect_args={"attrs_before": attrs_before},
    )
