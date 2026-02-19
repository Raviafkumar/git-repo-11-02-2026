import json
import sys
import ast
with open('output.json','r') as f:
    output = json.load(f) 
test_cases = ast.literal_eval(output['test_case_result']['result'])    

for test_case in test_cases:
    if test_case['test case result'] =='failed':
        print(f"Below test case failed: {test_case['test case desc']}")
        sys.exit(1)
print("All Test cases passed successfully. Deployment will start Now.")
