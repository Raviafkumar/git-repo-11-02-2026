output = {
  "url": "https://dbc-137322c2-693c.cloud.databricks.com/?o=1933389633836741#job/192168959172114/run/196040833395043",
  "test_case_result": "[{'test case desc': 'sum of amount in bronze for year 2026', 'test case result': 'passed'}, {'test case desc': 'sum of amount in silver for year 2026', 'test case result': 'failed'}]"
}
import ast 
result = ast.literal_eval(output['test_case_result'])
failed_test_cases = [x for x in result if x['test case result'] == 'failed']
if failed_test_cases:
    for test_case in failed_test_cases:
        print(f"Below test case failed: {test_case['test case desc']}")
    

# print(type(result))
# print(result[0])
# print(result[1])