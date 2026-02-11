spark.sql("""
          create or replace table customer_transaction_bronze 
          as
          select 'Invoice_1' as invoice_id,'2025-05-20' as invoice_date , 1000.123 as total_amount , 'mike' as customer_name
          union all
          select 'Invoice_2' as invoice_id,'2025-05-18' as invoice_date , 200.123 as total_amount , 'sam' as customer_name
          union all
          select 'Invoice_3' as invoice_id,'2025-05-17' as invoice_date , 99.123 as total_amount , 'sam' as Tina
          """)
spark.sql("""
          create or replace table customer_transaction_silver
          as
          select 'Invoice_1' as invoice_id,'2025-05-20' as invoice_date , 1000.123 as total_amount , 'mike' as customer_name
          union all
          select 'Invoice_2' as invoice_id,'2025-05-18' as invoice_date , 200.123 as total_amount , 'sam' as customer_name
          union all
          select 'Invoice_3' as invoice_id,'2025-05-17' as invoice_date , 99.123 as total_amount , 'sam' as Tina
          """)
queries = [{'query_desc':'sum of amount in bronze for year 2026',
            'query':'select coalesce(cast(sum(total_amount)as int),0) total_amount from customer_transaction_bronze where year(invoice_date) = 2026',
            'expected_value':0
            },
           {'query_desc':'sum of amount in silver for year 2026',
            'query':'select coalesce(cast(sum(total_amount)as int),0) total_amount from customer_transaction_silver where year(invoice_date) = 2026',
            'expected_value':122
            }
           ]

def execute_test_cases(queries):
    test_case_result = []
    for query in queries:
        try:
            test_case_desc =query['query_desc']
            assert spark.sql(query['query']).collect()[0][0] == query['expected_value'] ,f"{query['query_desc']} is failed"
            test_case_output = 'passed'
        except Exception as msg:
            test_case_output = 'failed'
        finally:
            test_case_result.append({'test case desc': test_case_desc,'test case result':test_case_output})

    return test_case_result

import json
dbutils.notebook.exit(json.dumps(execute_test_cases(queries)))
        
