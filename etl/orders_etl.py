import html
from typing import Dict, List

import pandas as pd
from pandas import DataFrame, Series

from etl.bq_client import BqClient


class OrdersEtl:
    """
    Loads processed Orders and Products data into Google BigQuery.

    Cleans, normalizes and merges two data sets into a single one prior to loading.
    """

    READ_SCHEMA = {
        "products": ["product_id", "price", "goods_group", "manufacturer"],
        "orders": [
            "order_source_id",
            "order_created_datetime",
            "customer_id",
            "status",
            "sum",
            "quantity",
            "name",
            "surname",
            "patronymic",
            "product_id",
        ],
    }

    TYPES_SCHEMA = {
        "orders": {
            "order_created_datetime": "datetime64[ns]",
            "status": "category",
            "sum": "float",
            "order_source_id": "int",
            "customer_id": "int",
            "quantity": "int",
            "product_id": "int",
        },
        "products": {
            "product_id": "int",
            "price": "float",
            "goods_group": "category",
            "manufacturer": "category",
        },
    }

    def __init__(
        self, orders_csv: str, products_csv: str, bq_table_name: str, bq_project_id=None
    ):
        """
        Instantiate etl runner.

        :param orders_csv: Path to Orders data csv file.
        :param products_csv: Path to Products data csv file.
        :param bq_project_id: GCP project ID. Optional when available from the environment.
        :param bq_table_name: Name of table to be written, in the form dataset.tablename.
        """
        self.orders_csv = orders_csv
        self.products_csv = products_csv
        self.bq_project_id = bq_project_id
        self.bq_table_name = bq_table_name
        self.output_df = None
        self.products_df = None
        self.orders_df = None

    def run(self):
        """Class entry point that executes the logic."""
        orders = pd.read_csv(self.orders_csv, usecols=self.READ_SCHEMA.get('orders'))
        products = pd.read_csv(self.products_csv, usecols=self.READ_SCHEMA.get('products'))

        self.orders_df = self.cast_orders_types(orders).drop_duplicates(
            subset=["order_source_id", "product_id"], keep="first"
        )
        self.orders_df.loc[:, ["name", "surname", "patronymic"]] = self.orders_df[
            ["name", "surname", "patronymic"]
        ].apply(self.clean_names, axis=0)
        self.products_df = self.cast_products_types(products).drop_duplicates(
            subset="product_id", keep="first"
        )
        self.output_df = self.join_frames(
            orders_df=self.orders_df, products_df=self.products_df
        )

        bq_client = BqClient(project_id=self.bq_project_id)
        bq_client.upload(table_name=self.bq_table_name, df=self.output_df)

    def find_similar_products(
        self, target_id: int, candidate_ids: List[int]
    ) -> Dict[int, int]:
        """
        Scores candidate products based on their similarity to the target product.

        :param target_id: Product ID that has to be compared against candidate products.
        :param candidate_ids: List of products IDs that need to be scored.
        :return: Dict of candidate product IDs and their scores.
        """
        pass

    def cast_orders_types(self, orders_df: DataFrame) -> DataFrame:
        """
        Casts DataFrame types to the ones described with SCHEMA.

        :param orders_df: Orders DataFrame to be casted.
        :return: Casted Orders DataFrame.
        """
        orders_df.loc[:, "sum"] = orders_df["sum"].str.replace(r",", ".", regex=True)
        orders_df.loc[:, "product_id"] = orders_df.product_id.str.replace(
            r"\D", "", regex=True
        )
        orders_df = orders_df.astype(self.TYPES_SCHEMA.get("orders"))
        return orders_df

    def cast_products_types(self, products_df: DataFrame) -> DataFrame:
        """
        Casts DataFrame types to the ones described with SCHEMA.

        :param products_df: Products DataFrame to be casted.
        :return: Casted Products DataFrame.
        """
        products_df = products_df.astype(self.TYPES_SCHEMA.get("products"))
        return products_df

    def clean_names(self, df_column: Series) -> Series:
        """
        Cleans any of name/surname/patronymic column.

        Removes digits, spaces, dashes.
        Removes values consisting of one letter.
        Removes values consisting of vowels.
        Remove values consisting of consonants.

        :param df_column: Series representing DataFrame column of names, e.g. surname.
        :return: Series of cleaned names.
        """
        pattern = r"\d|\s|^(-)$|(^\w{1}$)|(^[aoueiyаяєоуиіїе]{0,}$)|(^[^aoueiyаяєоуиіїе]{0,}$)"
        cleaned_column = (
            df_column.apply(html.unescape)
            .str.lower()
            .replace(r"\d", "", regex=True)
            .replace(pattern, "", regex=True)
        )
        return cleaned_column

    def join_frames(self, orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
        """Left joins Products DataFrame to Orders DataFrame"""
        return orders_df.merge(
            products_df, how="left", left_on="product_id", right_on="product_id"
        )


if __name__ == "__main__":
    orders_uploader = OrdersEtl(
        orders_csv="./input_data/orders_s.csv",
        products_csv="./input_data/products_s.csv",
        bq_project_id="neu-current",
        bq_table_name="orders.orders_denormalized",
    )
    orders_uploader.run()
