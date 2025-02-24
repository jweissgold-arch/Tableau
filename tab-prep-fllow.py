import tableauserverclient as TSC
import os

# --- CONFIGURATION ---
SOURCE_SERVER_URL = "https://linkprotect.cudasvc.com/url?a=https%3a%2f%2fawdata.mbta.com&c=E,1,bB4AVBLqx98JDbMzpEI79lVrk5DduMLKtFqMM_YTs35EYrAC44E2c88YuC_tI3khec58Yc3nfIGz3vFlFX91sJbxwfArvNcd-GUjHcwfw9j_og,,&typo=1"
SOURCE_USERNAME = ""
SOURCE_PASSWORD = ""
SOURCE_SITE = ""  # Only OP site

DEST_SERVER_URL = "https://linkprotect.cudasvc.com/url?a=https%3a%2f%2fsso.online.tableau.com%2f&c=E,1,lM2T1xGVe4r18Vgj_iLPleNcPyJN6svDgzaFS6obMkpEBIRZI4xUqPIGiT2d5SXVpjwiQ2Cute4EPpQQH578qKmtskZ1DBE5cYqgQKl-w4MP&typo=1"
DEST_USERNAME = ""
DEST_PASSWORD = ""
DEST_SITE = "OPMI"  # Leave empty for default site

TEMP_FOLDER = "temp_prep_flows"
os.makedirs(TEMP_FOLDER, exist_ok=True)

def get_prep_flows(server, auth):
    """Retrieve all Prep flows with owners and schedules."""
    print("\nFetching Prep flows from OP site...")
    prep_flows = []
    with server.auth.sign_in(auth):
        server._session.verify = False  # Disable SSL verification
        all_flows, _ = server.flows.get()
        for flow in all_flows:
            owner = server.users.get_by_id(flow.owner_id).name
            schedule = get_flow_schedule(server, https://linkprotect.cudasvc.com/url?a=https%3a%2f%2fflow.id&c=E,1,OJL5dwwYznf1CQno0MOEk9YXxZe5Op5czA5cseluRyIALkLuQETbB2KgqI-eoXqUBAWVWqKWRcycj7hrKS8mNGejPGvTrHK3hQl3Vk7Mpb2ZcyTjNc28END3wHEe&typo=1)
            flow_path = f"{flow.project_name}/{https://linkprotect.cudasvc.com/url?a=https%3a%2f%2fflow.name&c=E,1,Srnd_3wIWgt8anHXNNoGIkiPpFW1Gap11ZbbmrS5JYeXVdWGddyOYplZ1ss72ZYIsic1-_CZMfVSq3LJmPBHKctxSiMvsG-oA2ab6kwtWIwCfBs,&typo=1}"
            prep_flows.append((https://linkprotect.cudasvc.com/url?a=https%3a%2f%2fflow.id&c=E,1,C8LTnwRvAGSS5Re_cTQtmsfvsoq2ZayYYDAPA9wy1tiSd8N8OLkO-Jb_nHhseZiQlWvC1o4qOmD1ZCNqrl6a3czHozkiiKSKWVqiUN1g_9FJ1WU7Ap2MzmErLQ,,&typo=1, https://linkprotect.cudasvc.com/url?a=https%3a%2f%2fflow.name&c=E,1,DG1s2oYCHvzORcMX1lINhrk_5TNNseyMtw19Hhh8WCBToT_DjSJnw-gyAfCjwmdp3lBe7VZppBGfPye6ApsN6ejwnjVFeyu5P9ng4Gfe9LnNbx0o&typo=1, flow_path, flow.project_name, owner, schedule))
    return prep_flows

def get_flow_schedule(server, flow_id):
    """Get the schedule of a given Prep flow."""
    try:
        tasks, _ = server.tasks.get()
        for task in tasks:
            if hasattr(task, "flow_id") and task.flow_id == flow_id:
                return task.schedule_id
    except Exception:
        pass
    return None

def ensure_project(dev_server, dev_auth, project_name):
    """Ensure project folder exists on Destination Server."""
    with dev_server.auth.sign_in(dev_auth):
        all_projects, _ = dev_server.projects.get()
        matching_projects = [p for p in all_projects if https://linkprotect.cudasvc.com/url?a=https%3a%2f%2fp.name&c=E,1,M8UWoRZwYQvtMVqwS0a-Dybn-QAldsAkjK2Chh_dKwHTWorv_TceZQUGPbBzvHCw2gqyE44zdLg3Ccph4AYL1JYdBAe0iRJviEfv7CqU0FyTNgTw&typo=1 == project_name]
        if matching_projects:
            return matching_projects[0].id
        new_project = TSC.ProjectItem(name=project_name)
        new_project = dev_server.projects.create(new_project)
        return https://linkprotect.cudasvc.com/url?a=https%3a%2f%2fnew_project.id&c=E,1,a4zhAQeTj4W0ftU0GuGfeZIKlJEdo8_DumjLar-DhxDXJ4He96UQ7BVTcg1CoorFMY3FRzEVKYbEX-kR4nX7pZWF0MA211hcTojJnEfne_b55PJ5tW02PGMcZBM,&typo=1

def publish_prep_flows(prep_flows, source_server, dest_server, source_auth, dest_auth):
    """Download and publish Prep flows."""
    with source_server.auth.sign_in(source_auth):
        for flow_id, flow_name, flow_path, project_name, owner_name, schedule_id in prep_flows:
            try:
                print(f"\nDownloading {flow_name} ...")

                # Ensure the filename does not have a duplicate .tflx extension
                temp_file = os.path.join(TEMP_FOLDER, flow_name)
                if not temp_file.endswith(".tflx"):
                    temp_file += ".tflx"

                # https://linkprotect.cudasvc.com/url?a=https%3a%2f%2fsource_server.flows.download&c=E,1,MuKibvB5Gw32ty3DX2HBcx7TkcfOaCI3HQQdho1tSbT9DC4wk11lCHzGiz_B7UrDRdGpi3al01uDVZ22R0qYVCks4rNBoOjQ5Qrd0l44Iag,&typo=1(flow_id, filepath=temp_file)
                downloaded_file = https://linkprotect.cudasvc.com/url?a=https%3a%2f%2fsource_server.flows.download&c=E,1,Cewd3HLP8Nu8LGon2A7vKhYROs05S-q_sJyVqALEer-OFnAv3kKXldacW94piex49kC4HiA9CuVOJMUviaitJIDeYjbh0vpi4S69se1gpw,,&typo=1(flow_id, filepath=TEMP_FOLDER)

                # Ensure we get the correct filename (Tableau might rename it)
                if downloaded_file:
                    temp_file = downloaded_file  # Use the actual downloaded file
                    print(f"Downloaded: {temp_file}")
                else:
                    print(f"Error: Flow {flow_name} was not downloaded correctly.")
                    continue  # Skip this flow

                # Ensure the file exists before publishing
                if not os.path.exists(temp_file):
                    print(f"Error: File {temp_file} not found after download.")
                    continue  # Skip to the next flow if the file is missing

                project_id = ensure_project(dest_server, dest_auth, project_name)

                with dest_server.auth.sign_in(dest_auth):
                    print(f"Publishing {flow_name} to Destination...")
                    # new_flow = TSC.FlowItem(project_id=project_id)
                    # with open(temp_file, 'rb') as file_obj:
                        # new_flow = dest_server.flows.publish(new_flow, file_obj, TSC.Server.PublishMode.Overwrite)
                    new_flow = TSC.FlowItem(name=flow_name, project_id=project_id)  # Assign the name
                    with open(temp_file, 'rb') as file_obj:
                        new_flow = dest_server.flows.publish(new_flow, file_obj, TSC.Server.PublishMode.Overwrite)
                    
                    dest_users, _ = dest_server.users.get()
                    owner = next((u for u in dest_users if https://linkprotect.cudasvc.com/url?a=https%3a%2f%2fu.name&c=E,1,AA2EiExHjAz7t87OJHfBj5tv11cDeFO0nYQbma-dq9lJGU_pUiS5Tp1y9hU-fuZ9qmKk4QaC6yr7wyCxQMeCjD-ggRSBncMQdikweS4Uq04zW7qE&typo=1 == owner_name), None)
                    if owner:
                        new_flow.owner_id = https://linkprotect.cudasvc.com/url?a=https%3a%2f%2fowner.id&c=E,1,B8X-UEQ5Cd7iK6sRenRxSt5B73cakgZgqpr5pfKlZbAmyiMx8gBhkNnjom9uHrV9bmDVp_6kfcZReVJymS-Orsh5kjijBTSWrg3EJK9atOEoxQ,,&typo=1
                        dest_server.flows.update(new_flow)

                    if schedule_id:
                        print(f"Assigning schedule {schedule_id} to {flow_name}...")
                        task_item = TSC.TaskItem(schedule_id=schedule_id, flow_id=https://linkprotect.cudasvc.com/url?a=https%3a%2f%2fnew_flow.id&c=E,1,Wn1G5OHVhnb-fFGRPFz88y-XTBDDBgz546tBP1wBqyDyg3tRRwuD2Ip97wxecV6Au9eQo2xr6GgCRLvMIV4YDSnsqGVZIdbWBaMZKViwxTvKnxlA&typo=1)
                        dest_server.tasks.create(task_item)

            except Exception as e:
                print(f"Error processing {flow_name}: {e}")

def main():
    source_auth = TSC.TableauAuth(SOURCE_USERNAME, SOURCE_PASSWORD, SOURCE_SITE)
    dest_auth = TSC.TableauAuth(DEST_USERNAME, DEST_PASSWORD, DEST_SITE)

    # Create server objects
    source_server = TSC.Server(SOURCE_SERVER_URL, use_server_version=True)
    dest_server = TSC.Server(DEST_SERVER_URL, use_server_version=True)

    # Disable SSL verification
    source_server.add_http_options({"verify": False})
    dest_server.add_http_options({"verify": False})

    # Sign in and migrate flows
    with source_server.auth.sign_in(source_auth), dest_server.auth.sign_in(dest_auth):
        prep_flows = get_prep_flows(source_server, source_auth)
        publish_prep_flows(prep_flows, source_server, dest_server, source_auth, dest_auth)

if __name__ == "__main__":
    main()
