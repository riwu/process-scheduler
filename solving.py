# scoring is sum of s_jt where j ranges across all machines and t ranges across all points in time
# s_jt = 1 + alpha * (exp^(max(0, c - beta)) -1)
# if deployment invalid, total cost is 1e9
from read_data import data_parsing_main
import random
import csv
from checker import compute_cost

jobs_with_resources_dict, job_objects_lst, machine_dict, machine_objects_lst = data_parsing_main()

lowest_cost = None

machine_objects_lst.reverse()
big_machine_cpu = machine_objects_lst[0].cpu_0
small_machine_cpu = machine_objects_lst[-1].cpu_0

def add_to_machine(job, only_use_new_machine = False):
    for machine in machine_objects_lst:
        if machine.add_job(job, only_use_new_machine):
            return

def allocate_jobs_to_new_machine(jobs, cpu):
    for i, job in enumerate(jobs):
        if job.max_cpu >= cpu:
            job_objects_lst_copy.pop(i)
            add_to_machine(job, True)

def random_algo():
    job_objects_lst_copy = list(job_objects_lst)
    random.shuffle(job_objects_lst_copy)

    allocate_jobs_to_new_machine(job_objects_lst_copy, big_machine_cpu * 0.2)
    allocate_jobs_to_new_machine(job_objects_lst_copy, small_machine_cpu * 0.5)

    for i, job in enumerate(job_objects_lst_copy):
        if DEBUG:
            print('i', i)
        add_to_machine(job)


while True:
    random_algo()

    cost = compute_cost(machine_objects_lst)
    if lowest_cost == None or cost < lowest_cost:
        lowest_cost = cost

        with open('output.csv', 'w') as csvfile:
            spamwriter = csv.writer(csvfile)

            for machine in machine_objects_lst:
                if DEBUG and machine.jobs:
                    print('jobs', len(machine.jobs))
                for job in machine.jobs:
                    if DEBUG:
                        print(job.inst_id + ', ' + machine.machine_id)
                    spamwriter.writerow(job.inst_id + ', ' + machine.machine_id)

    for machine in machine_objects_lst:
        machine.reset()

    print('cost', cost, lowest_cost)
    break

