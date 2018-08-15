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

def random_algo():
    random.shuffle(job_objects_lst)

    for i, job in enumerate(job_objects_lst):
        print('i', i)
        for machine in machine_objects_lst:
            if machine.add_job(job):
                break


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

