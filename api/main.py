import ansible_runner
from fastapi import FastAPI
from pydantic import BaseModel
import os

app = FastAPI()

class RunPlaybookRequest(BaseModel):
    playbook: str
    inventory: str = None
    ansible_cfg: str = None
    extravars: dict = {}

@app.post("/list-servers")
def run_playbook(req: RunPlaybookRequest):
    envvars = os.environ.copy()
    
    if req.ansible_cfg:
        envvars["ANSIBLE_CONFIG"] = req.ansible_cfg

    runner_args = {
        "private_data_dir": "../ansible",
        "playbook": req.playbook,
        "extravars": req.extravars,
        "envvars": envvars
    }
    
    if req.inventory:
        runner_args["inventory"] = req.inventory

    r = ansible_runner.run(**runner_args)

    servers = None

    for e in r.events:
        if e.get("event") == "runner_on_ok":
            res = e["event_data"].get("res", {})
            if "ansible_stats" in res:
                servers = res["ansible_stats"].get("data", {}).get("servers")

    return servers