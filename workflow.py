import requests
import time 
import sys


payload = {
  "tasks": [
    {
      "task_key": "step_run_test_case",
      "run_if": "ALL_SUCCESS",
      "spark_python_task": {
        "python_file": "/Workspace/Shared/proj_dbx_10_02_2026/run_test_cases.py",
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

while True:
    respone = requests.get(f"{dbx_url}/api/2.2/jobs/runs/get",headers = headers , params = {"run_id":run_id}).json()
    if respone['state']['life_cycle_state'] == "RUNNING":
        time.sleep(20)
    print("job completed successfully")
    task_run_id = respone['tasks'][0]['run_id']
    break
    
output = requests.get(f"{dbx_url}/api/2.2/jobs/runs/get-output",headers = headers , params = {"run_id":task_run_id}).json()
print(task_run_id)
print(output)
    

    
    

