import pexpect
import time
import re

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

def main():
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
            return

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
                        return

    # Change directory to self-supervised-diffusion
    cd_command = 'cd self-supervised-diffusion'
    run_command(ssh_session, cd_command, cigserver_prompt, timeout=120)

    # Execute ls command within SSH session and capture the output
    ls_command = 'ls'
    ls_output = run_command(ssh_session, ls_command, cigserver_prompt, timeout=120)

    # Close SSH session
    ssh_session.sendline('exit')
    ssh_session.expect(initial_prompt)  # Wait for the prompt to indicate the session is closing

    # Stop recording
    record.sendline('exit')
    record.expect(pexpect.EOF)
    time.sleep(1)  # Wait a bit for the recording to stop

    # Print the result of ls command
    print("ls Output:\n", ls_output)

if __name__ == "__main__":
    main()