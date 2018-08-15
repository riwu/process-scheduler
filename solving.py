# scoring is sum of s_jt where j ranges across all machines and t ranges across all points in time
# s_jt = 1 + alpha * (exp^(max(0, c - beta)) -1)
# if deployment invalid, total cost is 1e9
from read_data import data_parsing_main
import random
import csv

jobs_with_resources_dict, job_objects_lst, machine_dict, machine_objects_lst = data_parsing_main()

highest_score = 0

machine_objects_lst.reverse()

while True:
    random.shuffle(job_objects_lst)

    for i, job in enumerate(job_objects_lst[:10]):
        print('i', i)
        for machine in machine_objects_lst:
            if machine.add_job(job):
                break

    # score = compute_cost(machine_objects_lst)


    with open('output.csv', 'w') as csvfile:
        spamwriter = csv.writer(csvfile)

        for machine in machine_objects_lst:
            if machine.jobs:
                print('jobs', len(machine.jobs))
            for job in machine.jobs:
                print(job.inst_id + ', ' + machine.machine_id)
                spamwriter.writerow(job.inst_id, machine.machine_id)

    break

