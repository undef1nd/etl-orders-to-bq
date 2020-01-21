import pandas_gbq
from pandas import DataFrame


class BqClient:
    """
    Wrapper around pandas_gbq to interact with Google BigQuery.

    Uses credentials specified in GOOGLE_APPLICATION_CREDENTIALS env variable.
    """

    def __init__(self, project_id):
        """
        Instantiates the client.

        :param project_id: GCP project ID. Optional when available from the environment.
        """
        self.project_id = project_id

    def upload(self, table_name: str, df: DataFrame, **kwargs):
        """
        Writes DataFrame to Google BigQuery.

        :param table_name: Name of table to be written, in the form dataset.tablename.
        :param df: DataFrame to be written to a Google BigQuery table.
        """
        pandas_gbq.to_gbq(
            project_id=self.project_id,
            dataframe=df,
            destination_table=table_name,
            **kwargs
        )
