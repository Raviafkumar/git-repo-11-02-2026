import json
import sys
with open('output.json','r') as f:
    output = json.load(f) 

for test_case in output['test_case_result']['result']:
    if test_case['test case result'] =='failed':
        print(f"Below test case failed: {test_case['test case desc']}")
        sys.exit(1)
print("All Test cases passed successfully. Deployment will start Now.")
