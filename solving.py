# scoring is sum of s_jt where j ranges across all machines and t ranges across all points in time
# s_jt = 1 + alpha * (exp^(max(0, c - beta)) -1)
# if deployment invalid, total cost is 1e9
from read_data import data_parsing_main, debug
import random
import csv
from checker import compute_cost

DEBUG_PROGRESS = False

jobs_with_resources_dict, job_objects_lst, machine_dict, machine_objects_lst = data_parsing_main()

lowest_cost = None

machine_objects_lst.reverse()
big_machine_cpu = machine_objects_lst[0].cpu_0
small_machine_cpu = machine_objects_lst[-1].cpu_0

def debug_progress(*args):
    if DEBUG_PROGRESS:
        print(*args)

def add_to_machine(job, only_use_new_machine=False):
    for i, machine in enumerate(machine_objects_lst):
        debug('machine', i, machine.cpu_0)
        if machine.add_job(job, only_use_new_machine):
            return
        else:
            debug('machine not ok')
    print('out of machines job', job)
    raise Exception('Out of machines!')



def allocate_jobs_to_new_machine(jobs, cpu, prefix_str):
    left_over_jobs = []
    for i, job in enumerate(jobs):
        if job.max_cpu >= cpu:
            debug('job high', job.max_cpu, i)
            add_to_machine(job, True)
            print(prefix_str + " " + str(i))
        else:
            left_over_jobs.append(job)
    debug('left over', len(left_over_jobs))
    return left_over_jobs


def random_algo():
    job_objects_lst_copy = list(job_objects_lst)
    random.shuffle(job_objects_lst_copy)


    job_objects_lst_copy = allocate_jobs_to_new_machine(job_objects_lst_copy, big_machine_cpu * 0.2, "BIG ")
    debug_progress('big jobs done')
    job_objects_lst_copy = allocate_jobs_to_new_machine(job_objects_lst_copy, small_machine_cpu * 0.4, "MEDIUM ")
    debug_progress('medium jobs')
    for i, job in enumerate(job_objects_lst_copy):
        debug_progress('small ', i)
        add_to_machine(job)


while True:
    random_algo()

    cost = compute_cost(machine_objects_lst)
    if lowest_cost == None or cost < lowest_cost:
        lowest_cost = cost

        with open('output.csv', 'w') as csvfile:
            csv_writer = csv.writer(csvfile)

            for machine in machine_objects_lst:
                for job in machine.jobs:
                    debug(job.inst_id + ', ' + machine.machine_id)
                    csv_writer.writerow(job.inst_id + ', ' + machine.machine_id)

    for machine in machine_objects_lst:
        machine.reset()

    print('cost', cost, lowest_cost)
    break
