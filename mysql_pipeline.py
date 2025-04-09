"""The Intro Pipeline Template contains the example from the docs intro page"""

# mypy: disable-error-code="no-untyped-def,arg-type"

from typing import Optional
import pandas as pd
import sqlalchemy as sa

import dlt
from dlt.sources.helpers import requests
from dlt.sources.rest_api import rest_api_source

def load_nobel_data() -> None:

    source = rest_api_source({
        "client": {
            "base_url": "https://api.nobelprize.org/2.1/",
            },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "endpoint": {
                "params": {
                    'limit': 1000,
                    'offset': 0,
                },
            },
        },
        "resources": [
            # "posts" will be used as the endpoint path, the resource name,
            # and the table name in the destination. The HTTP client will send
            # a request to "https://api.example.com/posts".
            "nobelPrizes",

            # The explicit configuration allows you to link resources
            # and define query string parameters.
        ],
    })
    pipeline = dlt.pipeline(
    pipeline_name="rest_api_example",
    destination="clickhouse",
    dataset_name="rest_api_data",
    )

    load_info = pipeline.run(source)


def load_api_data() -> None:
    """Load data from the chess api, for more complex examples use our rest_api source"""

    # Create a dlt pipeline that will load
    # chess player data to the DuckDB destination
    pipeline = dlt.pipeline(
        pipeline_name="chess_pipeline", destination='clickhouse', dataset_name="player_data"
    )
    # Grab some player data from Chess.com API
    data = []
    for player in ["magnuscarlsen", "rpragchess"]:
        response = requests.get(f"https://api.chess.com/pub/player/{player}")
        response.raise_for_status()
        data.append(response.json())

    # Extract, normalize, and load the data
    load_info = pipeline.run(data, table_name="player")
    print(load_info)  # noqa: T201


def load_pandas_data() -> None:
    """Load data from a public csv via pandas"""

    owid_disasters_csv = ('nobel1.csv')
    df = pd.read_csv(owid_disasters_csv)

    pipeline = dlt.pipeline(
        pipeline_name="from_csv",
        destination='clickhouse',
        dataset_name="dlt_test",
    )
    load_info = pipeline.run(df, table_name="nobel",write_disposition="replace")

    print(load_info)  # noqa: T201


def load_sql_data() -> None:
    """Load data from a sql database with sqlalchemy, for more complex examples use our sql_database source"""

    # Use any SQL database supported by SQLAlchemy, below we use a public
    # MySQL instance to get data.
    # NOTE: you'll need to install pymysql with `pip install pymysql`
    # NOTE: loading data from public mysql instance may take several seconds
    engine = sa.create_engine("mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam")

    with engine.connect() as conn:
        # Select genome table, stream data in batches of 100 elements
        query = "SELECT * FROM genome LIMIT 1000"
        rows = conn.execution_options(yield_per=100).exec_driver_sql(query)

        pipeline = dlt.pipeline(
            pipeline_name="from_database",
            destination='clickhouse',
            dataset_name="genome_data",
        )

        # Convert the rows into dictionaries on the fly with a map function
        load_info = pipeline.run(map(lambda row: dict(row._mapping), rows), table_name="genome")

    print(load_info)  # noqa: T201


@dlt.resource(write_disposition="replace")
def github_api_resource(api_secret_key: Optional[str] = dlt.secrets.value):
    from dlt.sources.helpers.rest_client import paginate
    from dlt.sources.helpers.rest_client.auth import BearerTokenAuth
    from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

    url = "https://api.github.com/repos/dlt-hub/dlt/issues"

    # Github allows both authenticated and non-authenticated requests (with low rate limits)
    auth = BearerTokenAuth(api_secret_key) if api_secret_key else None
    for page in paginate(
        url, auth=auth, paginator=HeaderLinkPaginator(), params={"state": "open", "per_page": "100"}
    ):
        yield page


@dlt.source
def github_api_source(api_secret_key: Optional[str] = dlt.secrets.value):
    return github_api_resource(api_secret_key=api_secret_key)


def load_data_from_source():
    pipeline = dlt.pipeline(
        pipeline_name="github_api_pipeline", destination='clickhouse', dataset_name="github_api_data"
    )
    load_info = pipeline.run(github_api_source())
    print(load_info)  # noqa: T201


if __name__ == "__main__":
    #load_nobel_data()
    load_pandas_data()
    #load_sql_data()
