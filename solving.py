# scoring is sum of s_jt where j ranges across all machines and t ranges across all points in time
# s_jt = 1 + alpha * (exp^(max(0, c - beta)) -1)
# if deployment invalid, total cost is 1e9
from read_data import data_parsing_main

jobs_with_resources_dict, job_objects_lst, machine_dict, machine_objects_lst = data_parsing_main()
for job in job_objects_lst:
    machine_objects_lst[0].add_job(job)