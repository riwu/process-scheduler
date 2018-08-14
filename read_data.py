# Instance: An instance is the object to be scheduled to a machine. In production, an instance may be a docker container.
#
# App: Each instance belongs to one and only one App (short for “Application”). An App often includes many instances, and all instances belong to one App have same constraints.
#
# Machine: A machine is a server in our cluster. An instance is said to be scheduled to a machine if the instance is assigned to the machine.
#
# （1）Resource constraint. Each instance has resource constraint with 3 resource dimensions: CPU, Memory and Disk, in which CPU and Memory are given as time curves. Each value in the curve represents the amount of corresponding resource required by the instance at the time point. The constraint is that, at any timestamp T, on any machine A, for any resource type (CPU, Memory or Disk), the sum of resource required by the instances on this machine cannot be larger than the capacity of corresponding resource of the machine.
#
# （2）Special resource constraint.We have 3 special resource types reflecting the importance of instances: P type, M type, and PM type. They are independent resource constraints. For any given machine, the capacity for P, M and PM resources are specified and none can be violated, i.e. the sum of all instances’ P requirement on the machine cannot be larger the P capacity of the machine. Same for M, and PM.
#
# （3）Anti-affinity constraint (due to interference). As there are instances from different Apps running on the same machine, there are potential interference between instances from certain Apps and we need to try not put them on the same machine. Such anti-affinity constraint is described in the form of <App_A, App_B, k>, which means that if there is one instance from App_A on a machine, there could be at most k instances from App_B (k could be 0) on the same machine. App_A and App_B could be the same App id (e.g.  <App_A, App_A, k>), and this means at most k instances from App_A could be on one machine.
import pandas as pd
import os
from collections import defaultdict

pd.set_option('display.max_columns', None)
DATA_FOLDER = "data"

def get_csv(short_name, header_lst = None):
    loc = list(filter(lambda x: "csv" in x and short_name in x, os.listdir(DATA_FOLDER)))[0]
    full_loc = os.path.join(os.getcwd(), DATA_FOLDER, loc)
    print("For ", short_name, " Using file at: ", full_loc)
    res = pd.read_csv(full_loc,names=header_lst)
    return res

def pipe_separated_values_into_multiple_cols(df, prefix, original_col_name):
    return pd.concat([df.drop(original_col_name, axis=1),
               df[original_col_name].str.split("|", expand=True).applymap(lambda x: float(x)).rename(columns=lambda x: prefix + str(x))], axis=1)

def df_to_dict(df, col_as_index):
    d = {}
    records_lst = df.to_dict("records")
    for record in records_lst:
        record_id = record[col_as_index]

        if record_id in d:
            d[record_id]
    return d

# instance_id, app_id, id_of_machine this instance is running on
jobs = get_csv("instance", header_lst=["job_id", "app_id", "machine_id"])
# app id 1, app id 2, maximum # of instances of app id 2 that can be on same machine as at least one instance of app id 1
interference = get_csv("interference", header_lst=["app_id1", "app_id2", "max_app2"])
# machine id, cpu capacity, memory capacity, disk capacity, P capacity, M capacity, PM capacity
machine_resources = get_csv("machine_resources", header_lst=["machine_id", "cpu_capacity", "mem_capacity", "disk_capacity", "p_capacity", "m_capacity", "pm_capacity"])
# app_id, CPU curve, memory curve, disk req, P resource req, M resource req, PM resource req
raw_app_resources_1 = get_csv("app_resources", header_lst=["cpu_curve", "mem_curve", "disk", "p", "m", "pm"])
raw_app_resources_2 = pipe_separated_values_into_multiple_cols(raw_app_resources_1, "cpu_", "cpu_curve")
app_resources = pipe_separated_values_into_multiple_cols(raw_app_resources_2, "mem_", "mem_curve")

# print(jobs.head(1))
# print(interference.head(1))
# print(machine_resources.head(1))
# print(app_resources.head(1))

# jobs_map = df_to_dict(jobs, 'job_id'), defaultdict
# interference_map = df_to_dict(interference, 'app_id1')
print(interference.to_dict("records"))
# print(df_to_dict(jobs, 'job_id'), defaultdict)
print(interference_map)