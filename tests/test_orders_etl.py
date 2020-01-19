import pandas as pd
from pandas import np

from etl.orders_etl import OrdersEtl


test_orders_etl = OrdersEtl(
    orders_csv="./unknown.csv",
    products_csv="./unknown.csv",
    bq_table_name="none.none",
    bq_project_id="none",
)


def test_convert_orders_types():
    orders_df = pd.DataFrame(
        {
            "order_source_id": ["111", "222234", "49586"],
            "order_created_datetime": [
                "2019-04-28T18:20:05",
                "2019-04-20T16:04:04",
                "2019-04-23T17:26:06",
            ],
            "customer_id": ["5375", "3356", "786544"],
            "status": ["Paid", "Waiting_Accepted", "Failed"],
            "sum": ["540", "1300,65", "67"],
            "quantity": ["12", "23", "34"],
            "name": ["olena", "василь", "петро"],
            "surname": ["-", "петренко", "анрійко"],
            "patronymic": ["igorivna", "олександрович", "Маркіянович"],
            "product_id": ["345f56", "596904", "d59954"],
        }
    )
    expected_order_data_types = [
        "int64",
        "datetime64[ns]",
        "int64",
        "category",
        "float64",
        "int64",
        "object",
        "object",
        "object",
        "int64",
    ]
    actual_orders_dtypes = test_orders_etl._cast_orders_types(
        orders_df
    ).dtypes.to_list()
    actual_orders_dtypes = list(map(str, actual_orders_dtypes))
    assert expected_order_data_types == actual_orders_dtypes


def test_convert_products_types():
    products_df = pd.DataFrame(
        {
            "product_id": ["34556", "596904", "5678", "56332"],
            "price": ["45", "56.55", "10", "13.50"],
            "goods_group": [
                "Творчість та канцтовари",
                "Творчість та канцтовари",
                "Ігрові фігурки",
                "Іграшки для розвитку",
            ],
            "manufacturer": ["BIC", "BIC", "Bingo", "Hama"],
        }
    )

    expected_products_data_types = ["int64", "float64", "category", "category"]
    actual_products_dtypes = test_orders_etl._cast_products_types(
        products_df
    ).dtypes.to_list()
    actual_products_dtypes = list(map(str, actual_products_dtypes))
    assert expected_products_data_types == actual_products_dtypes


def test_clean_named():
    names = pd.Series(
        [
            "Olena",
            "-",
            "в&#039;ячеславівна",
            "я",
            "мар&#039;яна",
            "кіт",
            "ddd",
            "m",
            "с",
            "іванова-шипак",
        ]
    )
    expected_result = [
        "olena",
        "",
        "в'ячеславівна",
        "",
        "мар'яна",
        "кіт",
        "",
        "",
        "",
        "іванова-шипак",
    ]
    actual_result = test_orders_etl._clean_names(names).to_list()
    assert expected_result == actual_result


def test_join_frames():
    orders_df = pd.DataFrame(
        {
            "product_id": [34556, 59690, 33454, 45567],
            "order_source_id": [111, 222234, 49586, 456542],
            "status": ["Paid", "Waiting", "Paid", "Paid"],
        }
    )
    products_df = pd.DataFrame(
        {
            "product_id": [34556, 59690, 33454, 56332],
            "price": [45, 56.55, 10, 13.50],
            "goods_group": [
                "Творчість та канцтовари",
                "Творчість та канцтовари",
                "Ігрові фігурки",
                "Іграшки для розвитку",
            ],
            "manufacturer": ["BIC", "BIC", "Bingo", "Hama"],
        }
    )
    expected_df = pd.DataFrame(
        {
            "product_id": [34556, 59690, 33454, 45567],
            "order_source_id": [111, 222234, 49586, 456542],
            "status": ["Paid", "Waiting", "Paid", "Paid"],
            "price": [45, 56.55, 10, np.NaN],
            "goods_group": [
                "Творчість та канцтовари",
                "Творчість та канцтовари",
                "Ігрові фігурки",
                np.NaN,
            ],
            "manufacturer": ["BIC", "BIC", "Bingo", np.NaN],
        }
    )

    actual_df = test_orders_etl._join_frames(orders_df, products_df)
    pd.testing.assert_frame_equal(expected_df, actual_df)


def test_find_similar_products():
    test_orders_etl.products_df = pd.DataFrame(
        {
            "product_id": [
                536469,
                296597,
                385613,
                516423,
                516425,
                427227,
                439541,
                528462,
            ],
            "price": [749.0, 199.0, 199.0, 219.0, 299.0, 329.0, 810.0, 219.0],
            "goods_group": [
                "Для активного відпочинку",
                "Дитячі машинки",
                "Ігрові фігурки",
                "Дитячі машинки",
                "Дитячі машинки",
                "Дитячі машинки",
                "Дитячі машинки",
                "Дитячі машинки",
            ],
            "manufacturer": [
                "Bugs",
                "CARS",
                "CARS",
                "CARS",
                "CARS",
                "LENA",
                "LENA",
                "LENA",
            ],
        }
    )

    expected_candidate_scores = {
        536469: 0.08772,
        296597: 0.9726,
        385613: 0.4726,
        516423: 1,
        516425: 0.91973,
        427227: 0.6997,
        439541: 0.58111,
        528462: 0.8,
    }

    actual_candidate_scores = test_orders_etl.find_similar_products(
        target_id=516423,
        candidate_ids=[536469, 296597, 385613, 516423, 516425, 427227, 439541, 528462],
    )
    assert expected_candidate_scores == actual_candidate_scores
