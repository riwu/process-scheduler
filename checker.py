import math
from read_data import DURATION

ALPHA = 10
BETA = 0.5

def cost_function(cpu):
    return 1 + ALPHA * (math.exp(max(0, cpu - BETA)) - 1)

def score_machine(machine):
    machine_cost = 0
    for i in range(DURATION):
        cpu = getattr(machine, "CPU_"+i)
        machine_cost += cost_function(cpu)
    return machine_cost

def compute_cost(lst_of_machine_objects):
    cost = 0
    for machine in lst_of_machine_objects:
        cost += score_machine(machine)
    return cost
# s_jt = 1 + alpha * (exp^(max(0, c - beta)) -1)