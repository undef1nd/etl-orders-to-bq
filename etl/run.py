from etl.orders_etl import OrdersEtl

if __name__ == "__main__":
    orders_uploader = OrdersEtl(
        orders_csv="../input_data/orders_s.csv",
        products_csv="../input_data/products_s.csv",
        bq_project_id="neu-current",
        bq_table_name="orders.orders_denormalized",
    )
    orders_uploader.process()
    orders_uploader.write_to_bq(if_exists="replace")
    similarity = orders_uploader.find_similar_products(
        target_id=516423,
        candidate_ids=[536469, 296597, 385613, 516423, 516425, 427227, 439541, 528462],
    )
    print(similarity)
