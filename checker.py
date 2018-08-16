import math
from read_data import DURATION
import subprocess
import os
import re
from read_data import debug
ALPHA = 10
BETA = 0.5


def run_bash_command(bash_command):
    process = subprocess.Popen(bash_command.split(), stdout=subprocess.PIPE)
    output, error = process.communicate()
    decoded_output = output.decode("utf_8")
    dirty_score = decoded_output[decoded_output.rfind('选手所得分数为：'):]
    debug('raw output', output)
    debug('decoded output', decoded_output)
    output_utf = re.findall("\d+\.\d+E?\d?", dirty_score)
    if output_utf:
        output_utf = output_utf[0]
    else:
        output_utf = re.findall("\d+\.\d+E?\d?", decoded_output)[0]

    score = float(output_utf)
    debug('float output: ', score)
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
    if len(machine.jobs) == 0:
        return 0
    for i in range(DURATION):
        cpu = 1 - getattr(machine, "cpu_"+str(i))/machine.original_dict["cpu_"+str(i)]
        machine_cost += cost_function(cpu)
    return float(machine_cost/DURATION)

def compute_cost(lst_of_machine_objects):
    cost = 0
    for machine in lst_of_machine_objects:
        cost += score_machine(machine)
    return cost
