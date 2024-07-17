"""
This is NOT a normal pytest! This is a special client which will run a real live flow
as an integration test to ensure services are working properly.

Usage:

First, run this file locally with the `authorize` command
python test_xpcs.py authorize

This will spit out a large block of text, which can be placed into an environment variable
to run later. You can test locally by setting `export FLOW_PAYLOAD=...`, and running
the flow with `python test_xpcs.py run-test`.

To enable running integration tests with Github Actions, store the credentials here instead:

https://github.com/globus-gladier/gladier-xpcs/settings/secrets/actions

And the github workflow under .github/workflows/test_flow.yml will run it each day.
"""

import base64
import json
import os

import click
import fair_research_login.token_storage
import gladier
import gladier.tests
import globus_sdk

ENV_TOKENS = "FLOW_PAYLOAD"


def get_time(**data):
    import datetime

    return datetime.datetime.now().isoformat()


@gladier.generate_flow_definition
class GetTime(gladier.GladierBaseTool):
    funcx_functions = [get_time]
    required_input = ["funcx_endpoint_compute"]
    flow_input = {"funcx_endpoint_compute": "553e7b64-0480-473c-beef-be762ba979a9"}


@gladier.generate_flow_definition
class TestFlow(gladier.GladierBaseClient):
    gladier_tools = [GetTime]


def get_payload():
    """Fetch secrets from the environment"""
    # Unencode tokens from the env
    payload = os.getenv(ENV_TOKENS)
    payload = json.loads(base64.b64decode(payload.encode("utf-8")))

    # Get tokens by scope instead of resource server
    token_group = {}
    for scope in fair_research_login.token_storage.get_scopes(payload["tokens"]):
        for tgroup in payload["tokens"].values():
            if scope in tgroup["scope"].split():
                token_group[scope] = tgroup
    payload["tokens"] = token_group
    return payload


@click.group()
def cli():
    pass


@cli.command()
def authorize():
    test = TestFlow()
    required_inputs = set()
    for tool in test.tools:
        required_inputs = required_inputs.union(
            set(getattr(tool, "required_input", []))
        )
    inputs = test.get_input()
    user_inputs = {
        ri: input(f'Set value for "{ri}"> ')
        for ri in required_inputs
        if ri not in inputs["input"]
    }
    inputs["input"].update(user_inputs)
    payload = {
        "client_id": test.client_id,
        "tokens": test.get_native_client().load_tokens(),
        "input": inputs["input"],
        "config": dict(test.get_cfg(private=True)[test.section].items()),
    }
    click.echo(f'Add this to your Github Secrets under the name "{ENV_TOKENS}"')
    click.echo(base64.b64encode(bytes(json.dumps(payload), "utf-8")))


@cli.command()
def run_test():
    payload = get_payload()
    naac = globus_sdk.NativeAppAuthClient(client_id=payload["client_id"])
    authorizers = {
        scope: globus_sdk.RefreshTokenAuthorizer(data["refresh_token"], naac)
        for scope, data in payload["tokens"].items()
    }

    # Note! auto_registration=True will cause a re-registration per-run! This is due to
    # the test box getting differing checksums on function data, which makes it think the
    # function has changed since the last run. Right now this isn't a problem, and we're
    # allowing ourselves to re-run each time.
    test = TestFlow(authorizers=authorizers, auto_login=False, auto_registration=True)
    cfg = test.get_cfg(private=True)
    for k, v in payload["config"].items():
        cfg[test.section][k] = v
    cfg.save()

    for tool in test.tools:
        tool.funcx_functions = []

    result = test.run_flow()
    test.progress(result["run_id"])
    print(test.get_status(result["run_id"]))


if __name__ == "__main__":
    cli()
