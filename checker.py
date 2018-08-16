import math
from read_data import DURATION
import subprocess
import os
import re
ALPHA = 10
BETA = 0.5


def run_bash_command(bash_command):
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    decoded_output = output[output.rfind('?'):].decode("utf_8")
    print('raw output', output)
    print('decoded output', decoded_output)
    output_utf = re.findall("\d+\.\d+E?\d?", decoded_output)[0]
    print('float output: ', output_utf)
    score = float(output_utf)
    return score

def get_alibaba_score(instance_deploy_file, output_file ):
    return run_bash_command(
        "java -cp Judge/target/tianchi-evaluator-0.0.1-SNAPSHOT-jar-with-dependencies.jar " +
        "com.aliyun.tianchi.mgr.evaluate.evaluate.file.evaluator.AlibabaSchedulerEvaluatorRun " +
        os.path.join(os.getcwd(), "Judge/scheduling_app_resources.csv ") +
        os.path.join(os.getcwd(), "Judge/scheduling_machine_resources.csv ") +
        os.path.join(os.getcwd(), instance_deploy_file) + " " +
        os.path.join(os.getcwd(), "Judge/scheduling_app_interference.csv ") +
        os.path.join(os.getcwd(), output_file) + " "
    )
def cost_function(cpu):
    return 1 + ALPHA * (math.exp(max(0, cpu - BETA)) - 1)

def score_machine(machine):
    machine_cost = 0
    for i in range(DURATION):
        cpu = 1 - getattr(machine, "cpu_"+str(i))/machine.original_dict["cpu_"+str(i)]
        machine_cost += cost_function(cpu)
    return machine_cost

def compute_cost(lst_of_machine_objects):
    cost = 0
    for machine in lst_of_machine_objects:
        cost += score_machine(machine)
    return cost
