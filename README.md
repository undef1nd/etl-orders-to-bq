# orders-to-bq
###Tasks:

1. Export data from csv files to BigQuey using python. Filter and process user data in orders, bring names, surnames to unique form, remove duplicates. 
An output should be one table in BigQuery with clean data for each order and all product attributes from the products file.

2. 	Make a function that takes two parameters:
   
        * target product_id,
        * list of candidate product_ids as input
        
        Calculates and returns similarity of target id to a given list of candidate ids.
Defining what is "similarity" between products is part of the task. Hint: use product attributes or orders. 

        Choose one task from (3) or (4) to implement:

3. Write unit tests for tasks above, docstrings for implemented functions

4. Implement a search mechanism to find N most similar products for a given product_id. The search function should execute faster than in quadratic time.
