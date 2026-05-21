import os, argparse, requests, ast
from fabric_cicd import (
    FabricWorkspace,
    publish_all_items,
    unpublish_all_orphan_items,
    change_log_level,
    append_feature_flag,
)
from azure.identity import ClientSecretCredential
 
 
def get_workspace_id(p_ws_name, p_token):
    """Look up Fabric workspace GUID by display name."""
    url = "https://api.fabric.microsoft.com/v1/workspaces"
    headers = {
        "Authorization": f"Bearer {p_token.token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        for workspace in response.json()["value"]:
            if workspace["displayName"] == p_ws_name:
                return workspace["id"]
        return f"Error: Workspace {p_ws_name} could not be found."
    else:
        return f"Error: {response.status_code}, {response.text}"
 
 
append_feature_flag("enable_shortcut_publish")
change_log_level("DEBUG")
 
parser = argparse.ArgumentParser(description='Deploy Fabric items via fabric-cicd')
parser.add_argument('--aztenantid',     type=str, help='Azure tenant ID')
parser.add_argument('--azclientid',     type=str, help='SP client ID')
parser.add_argument('--azspsecret',     type=str, help='SP client secret')
parser.add_argument('--target_env',     type=str, help='Target environment: dev, test, or prod')
parser.add_argument('--items_in_scope', type=str, help='Fabric item types to deploy')
args = parser.parse_args()
 
# Authenticate using Service Principal credentials
token_credential = ClientSecretCredential(
    client_id=args.azclientid,
    client_secret=args.azspsecret,
    tenant_id=args.aztenantid,
)
 
# Read workspace name from GitHub Variable
# GitHub Variables are injected as env vars with exact names:
# DEV_WORKSPACE_NAME, TEST_WORKSPACE_NAME, PROD_WORKSPACE_NAME
tgtenv = args.target_env  # 'dev', 'test', or 'prod'
ws_env_var = f'{tgtenv.upper()}_WORKSPACE_NAME'
workspace_name = os.environ[ws_env_var]
print(f'  Deploying to environment : {tgtenv}')
print(f'  Workspace name           : {workspace_name}')
 
# Get a token and resolve workspace name to GUID
resource = 'https://api.fabric.microsoft.com/'
scope = f'{resource}.default'
token = token_credential.get_token(scope)
 
lookup_response = get_workspace_id(workspace_name, token)
if isinstance(lookup_response, str) and lookup_response.startswith("Error"):
    raise ValueError(f"{lookup_response}. Check workspace name in GitHub Variables.")
wks_id = lookup_response
print(f'  Workspace ID             : {wks_id}')
 
# Read git directory from GitHub Variable GIT_DIRECTORY
repository_directory = os.environ['GIT_DIRECTORY']
 
# Parse item types list
item_types = ast.literal_eval(args.items_in_scope)
 
# Initialise FabricWorkspace deployment context
target_workspace = FabricWorkspace(
    workspace_id=wks_id,
    environment=tgtenv,
    repository_directory=repository_directory,
    item_type_in_scope=item_types,
    token_credential=token_credential,
)
 
# Deploy all in-scope items and clean up orphans
publish_all_items(target_workspace)
unpublish_all_orphan_items(target_workspace)
print("Deployment complete.")
