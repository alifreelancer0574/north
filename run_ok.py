import subprocess
import time
import os

def check_process_running(process_name):
    output = subprocess.getoutput("ps aux | grep '" + process_name + "' | grep -v grep")
    return len(output.strip().split("\n")) > 0

def restart_process(script_name):
    os.system("pkill -f '" + script_name + "'")
    subprocess.Popen(["python3", script_name])

def calculate_time_difference():
    if os.path.exists("time.txt"):
        with open("time.txt", "r") as file:
            last_execution_time = datetime.strptime(file.read().strip(), "%Y-%m-%d %H:%M:%S")
            current_time = datetime.now()
            time_difference = (current_time - last_execution_time).total_seconds()
            return time_difference
    else:
        return float("inf")

def main():
    script_name = "mypython.py"
    while True:
        if not check_process_running(script_name):
            restart_process(script_name)
        time_difference = calculate_time_difference()
        if time_difference > 300:  # 5 minutes in seconds
            restart_process(script_name)
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()
