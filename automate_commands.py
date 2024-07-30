import os.path
import pickle
import pexpect
import time
import re
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# If modifying these SCOPES, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def run_command(session, command, prompt, timeout=60):
    """Send a command to the session and wait for the prompt."""
    session.sendline(command)
    session.expect(prompt, timeout=timeout)
    return session.before.decode()

def allocate_gpu(session, server_name, initial_prompt, cigserver_prompt):
    """Try to allocate GPU on the specified server."""
    gpu_command = f'bsub -m {server_name} -gpu "num=1" -Is /bin/bash'
    session.sendline(gpu_command)
    try:
        session.expect('<<Starting on', timeout=240)
        session.expect(cigserver_prompt, timeout=240)
        return True
    except pexpect.TIMEOUT:
        print(f"Failed to allocate on {server_name}")
        return False

def parse_nvidia_smi_output(output):
    """Parse nvidia-smi output to extract memory usage values."""
    match = re.search(r'(\d+)MiB\s*/\s*(\d+)MiB', output)
    if match:
        used_memory = int(match.group(1))
        total_memory = int(match.group(2))
        return used_memory, total_memory
    return None, None

def get_sheet_values():
    """Fetches values from Google Sheets."""
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'client_secret_348298541613-agbs5ilqjvg5vcqo7i01dbefd6afe1up.apps.googleusercontent.com.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    SPREADSHEET_ID = '1zSLSaFt1X1u4Qhj_wA0kZ6t2jtHylxreE1TVHJihC3g'
    ranges = ['Sheet1!P2:Q100', 'Sheet1!A2:G50']
    result = sheet.values().batchGet(spreadsheetId=SPREADSHEET_ID, ranges=ranges).execute()
    value_ranges = result.get('valueRanges', [])
    
    values_pq = value_ranges[0].get('values', []) if len(value_ranges) > 0 else []
    values_ag = value_ranges[1].get('values', []) if len(value_ranges) > 1 else []

    return values_pq, values_ag, service, SPREADSHEET_ID

def update_sheet(service, spreadsheet_id, range_name, values):
    """Update Google Sheets with the given values."""
    body = {
        'values': values
    }
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=range_name,
        valueInputOption="RAW", body=body).execute()

def find_highest_ema_file(files):
    """Find the highest ema_*.pt file."""
    ema_files = [f for f in files if re.match(r'ema_0\.9999_\d+\.pt', f)]
    if not ema_files:
        return None
    highest_ema_file = max(ema_files, key=lambda x: int(re.search(r'ema_0\.9999_(\d+)\.pt', x).group(1)))
    return highest_ema_file

def construct_command(directory, type, indi, indisteps, indinoise, num_samples, model_path):
    """Construct the command based on the parameters."""
    command = f'python fastmri_condititonal_sample.py --model_path {model_path} --type {type} --num_samples {num_samples}'
    if indi == 'TRUE':
        command += f' --indi --indisteps {indisteps} --indinoise {indinoise}'
    return command

def main():
    values_pq, values_ag, service, SPREADSHEET_ID = get_sheet_values()

    if not values_pq:
        print('No data found in range P2:Q100.')
        return

    for i, row in enumerate(values_pq):
        if not row or len(row) < 2:
            break  # Stop if there is no data or the data is insufficient in P or Q columns
        p_value = row[0].strip().upper() if len(row) > 0 else 'EMPTY'
        q_value = row[1].strip().upper() if len(row) > 1 else 'EMPTY'

        if p_value == 'TRUE' and q_value == 'FALSE':
            directory = values_ag[i][0] if i < len(values_ag) and values_ag[i] else 'experiment'
            type = values_ag[i][1] if i < len(values_ag) and len(values_ag[i]) > 1 else 'default'
            indi = values_ag[i][2] if i < len(values_ag) and len(values_ag[i]) > 2 else 'FALSE'
            indisteps = values_ag[i][3] if i < len(values_ag) and len(values_ag[i]) > 3 else ''
            indinoise = values_ag[i][4] if i < len(values_ag) and len(values_ag[i]) > 4 else ''
            num_samples = values_ag[i][5] if i < len(values_ag) and len(values_ag[i]) > 5 else '1'
            sample_dir_name = values_ag[i][6] if i < len(values_ag) and len(values_ag[i]) > 6 else 'samples'

            logfile = '/Users/davidgao/Downloads/consolelog.txt'

            # Start recording
            record = pexpect.spawn(f'script -q {logfile}', timeout=120)
            time.sleep(1)  # Wait a bit for the recording to start

            # Start SSH session and provide password
            ssh_command = 'ssh g.harry@ssh8.engr.wustl.edu'
            ssh_prompt = r"g.harry@ssh8.engr.wustl.edu's password:"
            initial_prompt = r'\$ '
            cigserver_prompt = r'\$ '

            ssh_session = pexpect.spawn(ssh_command, timeout=120)
            ssh_session.expect(ssh_prompt)
            ssh_session.sendline('ResearchPassword123')
            ssh_session.expect(initial_prompt)  # Wait for the shell prompt

            # Try to allocate GPU on cigserver5
            success = allocate_gpu(ssh_session, 'cigserver5', initial_prompt, cigserver_prompt)
            if not success:
                ssh_session.sendintr()  # Send Control-C to cancel the action
                ssh_session.expect(initial_prompt)
                
                # Try to allocate GPU on cigserver3
                success = allocate_gpu(ssh_session, 'cigserver3', initial_prompt, cigserver_prompt)
                if not success:
                    ssh_session.sendline('exit')
                    ssh_session.expect(pexpect.EOF)
                    record.sendline('exit')
                    record.expect(pexpect.EOF)
                    record.close()
                    print("Due to no servers being available, the job couldn't be completed.")
                    continue  # Move to the next row instead of return

            while True:
                # Execute nvidia-smi command within SSH session and capture the output
                nvidia_smi_command = 'nvidia-smi'
                ssh_session.sendline(nvidia_smi_command)
                ssh_session.expect(cigserver_prompt, timeout=120)
                nvidia_smi_output = ssh_session.before.decode()

                # Validate the nvidia-smi output
                if "NVIDIA-SMI" not in nvidia_smi_output:
                    print("nvidia-smi command failed.")
                    ssh_session.sendline('exit')
                    ssh_session.expect(initial_prompt)
                    record.sendline('exit')
                    record.expect(pexpect.EOF)
                    record.close()
                    return

                # Parse the nvidia-smi output to get memory values
                used_memory, total_memory = parse_nvidia_smi_output(nvidia_smi_output)
                print(f"Parsed Memory Values - Used: {used_memory}, Total: {total_memory}")

                if used_memory is not None and total_memory is not None:
                    diff = abs(total_memory - used_memory)
                    print(f"Used Memory: {used_memory} MiB, Total Memory: {total_memory} MiB, Difference: {diff} MiB")

                    if diff > 3000:
                        print("Success!")
                        break
                    else:
                        print("Memory difference is less than 3,000 MiB, restarting the process.")
                        ssh_session.sendline('exit')
                        ssh_session.expect(initial_prompt)
                        success = allocate_gpu(ssh_session, 'cigserver5', initial_prompt, cigserver_prompt)
                        if not success:
                            ssh_session.sendintr()  # Send Control-C to cancel the action
                            ssh_session.expect(initial_prompt)
                            success = allocate_gpu(ssh_session, 'cigserver3', initial_prompt, cigserver_prompt)
                            if not success:
                                ssh_session.sendline('exit')
                                ssh_session.expect(pexpect.EOF)
                                record.sendline('exit')
                                record.expect(pexpect.EOF)
                                record.close()
                                print("Due to no servers being available, the job couldn't be completed.")
                                continue  # Move to the next row instead of return

            # Change directory to self-supervised-diffusion/final_experiments/<directory>
            cd_command = f'cd self-supervised-diffusion/final_experiments/{directory}'
            run_command(ssh_session, cd_command, cigserver_prompt, timeout=120)

            # Execute ls command within SSH session and capture the output
            ls_command = 'ls'
            ls_output = run_command(ssh_session, ls_command, cigserver_prompt, timeout=120)

            # Find the highest ema_*.pt file
            files = ls_output.split()
            highest_ema_file = find_highest_ema_file(files)
            model_path = f'/project/cigserver3/export1/g.harry/self-supervised-diffusion/final_experiments/{directory}/{highest_ema_file}'

            # Construct the command
            command = construct_command(directory, type, indi, indisteps, indinoise, num_samples, model_path)
            print("Constructed Command:", command)

            # Update the Google Sheet
            command_cell = f'H{i+2}'
            model_cell = f'I{i+2}'
            update_sheet(service, SPREADSHEET_ID, command_cell, [[command]])
            update_sheet(service, SPREADSHEET_ID, model_cell, [[highest_ema_file]])
            update_sheet(service, SPREADSHEET_ID, f'Sheet1!Q{i+2}', [[True]])  # Use boolean True instead of string 'TRUE'

            # Close SSH session
            ssh_session.sendline('exit')
            ssh_session.expect(initial_prompt)  # Wait for the prompt to indicate the session is closing

            # Stop recording
            record.sendline('exit')
            record.expect(pexpect.EOF)
            time.sleep(1)  # Wait a bit for the recording to stop

if __name__ == '__main__':
    main()
