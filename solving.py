# scoring is sum of s_jt where j ranges across all machines and t ranges across all points in time
# s_jt = 1 + alpha * (exp^(max(0, c - beta)) -1)
# if deployment invalid, total cost is 1e9
from read_data import data_parsing_main, debug, CSV_FILE, DATA_FOLDER
import random
import csv
from checker import compute_cost, get_alibaba_score,score_machine
import time
import pprint

DEBUG_PROGRESS = True
COST_TO_REALLOCATE = 1.5

def debug_progress(*args):
    if DEBUG_PROGRESS:
        print(*args)

def add_to_machine_for_cpu_limit(job, csv_writer, only_use_new_machine=False, cpu_limit = 1):
    for i, machine in enumerate(machine_objects_lst):
        debug('machine', i, machine.cpu_0)
        if machine.add_job(job, only_use_new_machine, False, cpu_limit):
            csv_writer.writerow([job.inst_id, machine.machine_id])
            return True
        else:
            debug('machine not ok')
    debug('out of machines job', job)

def add_to_machine(job, csv_writer, only_use_new_machine=False):
    cpu_limits = [0.5, 1]
    for cpu_limit in cpu_limits:
        if add_to_machine_for_cpu_limit(job, csv_writer, only_use_new_machine, cpu_limit):
            return
    raise Exception('Out of machines!')


def fix_initial_allocation(machine_objects_lst, csv_writer):
    for m, machine in enumerate(machine_objects_lst):
        # print('m', m)
        cnt = 0
        if score_machine(machine) > COST_TO_REALLOCATE:
            cnt += 1
            for i, job in list(enumerate(machine.jobs)):
                machine.remove_job(job.inst_id)
                add_to_machine(job, csv_writer)
    debug_progress("Num of jobs fixed: ", cnt)
    print('done reallocating')

def allocate_jobs_to_new_machine(jobs, cpu, prefix_str, csv_writer):
    left_over_jobs = []
    for i, job in enumerate(jobs):
        if job.max_cpu >= cpu:
            debug('job high', job.max_cpu, i)
            add_to_machine(job, csv_writer, False)
            debug_progress(prefix_str + " " + str(i))
        else:
            left_over_jobs.append(job)
    debug_progress('left over', len(left_over_jobs))
    return left_over_jobs


def random_algo(csv_writer):
    job_objects_lst_copy = list(remaining_jobs)
    random.shuffle(job_objects_lst_copy)

    job_objects_lst_copy = allocate_jobs_to_new_machine(job_objects_lst_copy, big_machine_cpu * 0.4, "BIG ", csv_writer)
    debug_progress('big jobs done')
    # job_objects_lst_copy = allocate_jobs_to_new_machine(job_objects_lst_copy, small_machine_cpu * 0.4, "MEDIUM ", csv_writer)
    # debug_progress('medium jobs')
    for i, job in enumerate(job_objects_lst_copy):
        debug_progress('small ', i)
        add_to_machine(job, csv_writer)

jobs_with_resources_dict, job_objects_lst, machine_dict, machine_objects_lst = data_parsing_main()
machine_objects_lst.reverse()
big_machine_cpu = machine_objects_lst[0].cpu_0
small_machine_cpu = machine_objects_lst[-1].cpu_0

lowest_cost = None
file_name = None

while True:
    timestamp = str(time.time()).replace('.', '')
    output_csv = 'Judge/outputSample' + timestamp + '.csv'


    with open(output_csv, 'w') as csvfile:
        csv_writer = csv.writer(csvfile)
        remaining_jobs = []
        for i, job in enumerate(job_objects_lst):
            debug_progress("Initial allocation of Job ", i)
            job_added = False
            for j, machine in enumerate(machine_objects_lst):
                if job.machine_id == machine.machine_id:
                    machine.add_job(job, False, True)
                    job_added = True
            if not job_added:
                remaining_jobs.append(job)

        fix_initial_allocation(machine_objects_lst, csv_writer)
        debug_progress('remaining jobs', len(remaining_jobs))
        try:
            random_algo(csv_writer)
        except:
            pass

    cost = get_alibaba_score(DATA_FOLDER + "/" + CSV_FILE, output_csv)
    if lowest_cost == None or cost < lowest_cost:
        lowest_cost = cost
        file_name = output_csv
        # with open(output_csv, 'w') as csvfile:
        #     csv_writer = csv.writer(csvfile)
        #
        #     for machine in machine_objects_lst:
        #         for job in machine.jobs:
        #             debug(job.inst_id + ',' + machine.machine_id)
        #             csv_writer.writerow([job.inst_id, machine.machine_id])

    print('cost', cost, output_csv, lowest_cost, file_name)
    print("our computed score: ", compute_cost(machine_objects_lst))
    print("_________________________")

    for machine in machine_objects_lst:
        machine.reset()

    break
