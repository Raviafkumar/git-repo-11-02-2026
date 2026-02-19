import requests
import time 
import sys
import json 
import ast 


payload = {
  "tasks": [
    {
      "task_key": "step_run_test_case",
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": "/Workspace/Users/ravi.af.kumar@gmail.com/test_case",
        "source": "WORKSPACE"
      },
      "timeout_seconds": 0,
      "email_notifications": {},
      "environment_key": "Default"
    }
  ],
  "environments": [
    {
      "environment_key": "Default",
      "spec": {
        "environment_version": "4"
      }
    }
  ],
  "performance_target": "PERFORMANCE_OPTIMIZED"
}


token = sys.argv[1]
headers = {"authorization": f"Bearer {token} "}

dbx_url = 'https://dbc-137322c2-693c.cloud.databricks.com'

respone = requests.post(f"{dbx_url}/api/2.2/jobs/runs/submit",json = payload,headers = headers).json()
run_id = respone["run_id"]
task_run_id = None
while True:
    respone = requests.get(f"{dbx_url}/api/2.2/jobs/runs/get",headers = headers , params = {"run_id":run_id}).json()
    if respone['state']['life_cycle_state'] == "RUNNING":
        time.sleep(20)
    task_run_id = respone['tasks'][0]['run_id']
    break
    
output = requests.get(f"{dbx_url}/api/2.2/jobs/runs/get-output",headers = headers , params = {"run_id":task_run_id}).json()


parsed_output = {"url":output['metadata']['run_page_url'],
                 'test_case_result':output['notebook_output']}

#

# dict_output = ast.literal_eval(parsed_output['test_case_result'])
# failed_test_cases = [x for x in dict_output if x['test case result'] == 'failed']

# if failed_test_cases:
#     for test_case in failed_test_cases:
#         print(f"Below test case failed: {test_case['test case desc']}")

with open("output.json","w") as f:
    f.write(json.dumps(parsed_output,indent = 2 )) 
   

