import os
import sys
import time
import json

from fair_research_login import NativeClient

from globus_automate_client import FlowsClient
from globus_automate_client.flows_client import (
    MANAGE_FLOWS_SCOPE,
    AllowedAuthorizersType,
)
from globus_automate_client.token_management import CLIENT_ID
from globus_sdk import (AccessTokenAuthorizer, RefreshTokenAuthorizer, AuthClient,
                        NativeAppAuthClient)


class MemStorage(object):

    def __init__(self, tokens=None):
        self.tokens = tokens or {}

    def write_tokens(self, tokens):
        self.tokens = tokens

    def read_tokens(self):
        return self.tokens

    def clear_tokens(self):
        self.tokens = {}


def authorizer_retriever(
    flow_url: str, flow_scope: str, client_id: str
) -> AllowedAuthorizersType:
    """
    This callback will be called when attempting to interacting with a
    specific Flow. The callback will receive the Flow url, Flow scope, and
    client_id and can choose to use some, all or none of the kwargs. This is
    expected to return an Authorizer which can be used to make authenticated
    calls to the Flow.

    The method used to acquire valid credentials is up to the user. This example
    naively creates an Authorizer using the same token everytime.
    """
    flow_token = os.environ.get("FLOW_TOKEN")
    auth_client = NativeAppAuthClient(client_id=CLIENT_ID)
    return RefreshTokenAuthorizer(flow_token, auth_client)

def run_flow():

    scopes = ["https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/manage_flows",
              "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/view_flows",
              "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/run",
              "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/run_status"]
    tokens = os.environ.get("XPCS_TOKENS")

    tokens = {'flows_automated_tests': {
                  'scope': " ".join(scopes),
                  'access_token': tokens.split("::")[0],
                  'refresh_token': tokens.split("::")[1],
                  'expires_at_seconds': int(tokens.split("::")[2]),
                  'resource_server': 'flows_automated_tests'
              }}
    app = NativeClient(client_id=CLIENT_ID, default_scopes=scopes,
                       token_storage=MemStorage(tokens=tokens))
    flows_service_authorizer = app.get_authorizer(tokens['flows_automated_tests'])
    print(f'Loaded tokens for: {app.load_tokens().keys()}')

    fc = FlowsClient.new_client(    
        client_id=CLIENT_ID,
        authorizer_callback=authorizer_retriever,
        authorizer=flows_service_authorizer,
    )

    flow_input = {
  "Transfer1Input": {
    "source_endpoint_id": "b0e921df-6d04-11e5-ba46-22000b92c6ec",
    "destination_endpoint_id": "08925f04-569f-11e7-bef8-22000b9a448b",
    "transfer_items": [
      {
        "source_path": "/data/xpcs8/2019-1/comm201901/cluster_results/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf",
        "destination_path": "/projects/APSDataAnalysis/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf",
        "recursive": False
      },
      {
        "source_path": "/data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm",
        "destination_path": "/projects/APSDataAnalysis/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm",
        "recursive": False
      }
    ]
  },
  "Transfer2Input": {
    "source_endpoint_id": "08925f04-569f-11e7-bef8-22000b9a448b",
    "destination_endpoint_id": "b0e921df-6d04-11e5-ba46-22000b92c6ec",
    "transfer_items": [
      {
        "source_path": "/projects/APSDataAnalysis/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf",
        "destination_path": "/data/xpcs8/2019-1/comm201901/ALCF_results/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf",
        "recursive": False
      }
    ]
  },
  "Exec1Input": {
    "tasks": [
      {
        "endpoint": "9f84f41e-dfb6-4633-97be-b46901e9384c",
        "func": "7333e9d3-26b3-4ec8-acb6-e1efe4f4af96",
        "payload": {
          "data": {
            "hdf": "/projects/APSDataAnalysis/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf",
            "imm": "/projects/APSDataAnalysis/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm",
            "metadata": {
              "reprocessing": {
                "source_endpoint": "b0e921df-6d04-11e5-ba46-22000b92c6ec",
                "source_hdf_abspath": "/data/xpcs8/2019-1/comm201901/cluster_results/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf",
                "source_imm_abspath": "/data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm"
              }
            },
            "metadata_file": "/projects/APSDataAnalysis/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.json"
          }
        }
      }
    ]
  },
  "Exec2Input": {
    "tasks": [
      {
        "endpoint": "9f84f41e-dfb6-4633-97be-b46901e9384c",
        "func": "179695a4-b6da-4b64-8898-f406a641699f",
        "payload": {
          "data": {
            "hdf": "/projects/APSDataAnalysis/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf",
            "imm": "/projects/APSDataAnalysis/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm",
            "metadata": {
              "reprocessing": {
                "source_endpoint": "b0e921df-6d04-11e5-ba46-22000b92c6ec",
                "source_hdf_abspath": "/data/xpcs8/2019-1/comm201901/cluster_results/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf",
                "source_imm_abspath": "/data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm"
              }
            },
            "metadata_file": "/projects/APSDataAnalysis/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.json"
          }
        }
      }
    ]
  },
  "Exec3Input": {
    "tasks": [
      {
        "endpoint": "6c4323f4-a062-4551-a883-146a352a43f5",
        "func": "c7af9be3-3637-4eb4-a026-b9571426312f",
        "payload": {
          "data": {
            "hdf": "/projects/APSDataAnalysis/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf",
            "imm": "/projects/APSDataAnalysis/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm",
            "metadata": {
              "reprocessing": {
                "source_endpoint": "b0e921df-6d04-11e5-ba46-22000b92c6ec",
                "source_hdf_abspath": "/data/xpcs8/2019-1/comm201901/cluster_results/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf",
                "source_imm_abspath": "/data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm"
              }
            },
            "metadata_file": "/projects/APSDataAnalysis/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.json"
          }
        }
      }
    ]
  }
}
    flow_id = os.environ.get("FLOW_ID", "ffb59a75-8e26-4a95-a6bf-5717a6721ae5")

    running_flow = fc.run_flow(
        flow_id, None, flow_input
    )

    print(running_flow)
    assert "action_id" in running_flow

    flow_action_id = running_flow['action_id']
    flow_status = running_flow['status']
    print(f'Flow action started with id: {flow_action_id}')
    for i in range(270):
        if flow_status == "ACTIVE":
            running_flow = fc.flow_action_status(flow_id, None, flow_action_id)
            flow_status = running_flow['status']
            time.sleep(10)

    assert flow_status == "SUCCEEDED";
  
if __name__ == "__main__":
    run_flow() 
