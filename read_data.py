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
import copy

pd.set_option('display.max_columns', None)
DATA_FOLDER = "Judge"
DURATION = 98
ELEMENTS_TO_UPDATE = ['disk', 'p', 'm', 'pm', 'cpu_0', 'cpu_1', 'cpu_2', 'cpu_3', 'cpu_4', 'cpu_5', 'cpu_6', 'cpu_7',
                      'cpu_8', 'cpu_9', 'cpu_10', 'cpu_11', 'cpu_12', 'cpu_13', 'cpu_14', 'cpu_15', 'cpu_16', 'cpu_17',
                      'cpu_18', 'cpu_19', 'cpu_20', 'cpu_21', 'cpu_22', 'cpu_23', 'cpu_24', 'cpu_25', 'cpu_26',
                      'cpu_27', 'cpu_28', 'cpu_29', 'cpu_30', 'cpu_31', 'cpu_32', 'cpu_33', 'cpu_34', 'cpu_35',
                      'cpu_36', 'cpu_37', 'cpu_38', 'cpu_39', 'cpu_40', 'cpu_41', 'cpu_42', 'cpu_43', 'cpu_44',
                      'cpu_45', 'cpu_46', 'cpu_47', 'cpu_48', 'cpu_49', 'cpu_50', 'cpu_51', 'cpu_52', 'cpu_53',
                      'cpu_54', 'cpu_55', 'cpu_56', 'cpu_57', 'cpu_58', 'cpu_59', 'cpu_60', 'cpu_61', 'cpu_62',
                      'cpu_63', 'cpu_64', 'cpu_65', 'cpu_66', 'cpu_67', 'cpu_68', 'cpu_69', 'cpu_70', 'cpu_71',
                      'cpu_72', 'cpu_73', 'cpu_74', 'cpu_75', 'cpu_76', 'cpu_77', 'cpu_78', 'cpu_79', 'cpu_80',
                      'cpu_81', 'cpu_82', 'cpu_83', 'cpu_84', 'cpu_85', 'cpu_86', 'cpu_87', 'cpu_88', 'cpu_89',
                      'cpu_90', 'cpu_91', 'cpu_92', 'cpu_93', 'cpu_94', 'cpu_95', 'cpu_96', 'cpu_97', 'mem_0', 'mem_1',
                      'mem_2', 'mem_3', 'mem_4', 'mem_5', 'mem_6', 'mem_7', 'mem_8', 'mem_9', 'mem_10', 'mem_11',
                      'mem_12', 'mem_13', 'mem_14', 'mem_15', 'mem_16', 'mem_17', 'mem_18', 'mem_19', 'mem_20',
                      'mem_21', 'mem_22', 'mem_23', 'mem_24', 'mem_25', 'mem_26', 'mem_27', 'mem_28', 'mem_29',
                      'mem_30', 'mem_31', 'mem_32', 'mem_33', 'mem_34', 'mem_35', 'mem_36', 'mem_37', 'mem_38',
                      'mem_39', 'mem_40', 'mem_41', 'mem_42', 'mem_43', 'mem_44', 'mem_45', 'mem_46', 'mem_47',
                      'mem_48', 'mem_49', 'mem_50', 'mem_51', 'mem_52', 'mem_53', 'mem_54', 'mem_55', 'mem_56',
                      'mem_57', 'mem_58', 'mem_59', 'mem_60', 'mem_61', 'mem_62', 'mem_63', 'mem_64', 'mem_65',
                      'mem_66', 'mem_67', 'mem_68', 'mem_69', 'mem_70', 'mem_71', 'mem_72', 'mem_73', 'mem_74',
                      'mem_75', 'mem_76', 'mem_77', 'mem_78', 'mem_79', 'mem_80', 'mem_81', 'mem_82', 'mem_83',
                      'mem_84', 'mem_85', 'mem_86', 'mem_87', 'mem_88', 'mem_89', 'mem_90', 'mem_91', 'mem_92',
                      'mem_93', 'mem_94', 'mem_95', 'mem_96', 'mem_97']
NUM_OF_JOBS = 68224
NUM_OF_LIMITED_JOBS = 9338
CPU_SOFT_LIMIT = 0.5
DEBUG = False


def debug(*msg):
    if DEBUG:
        print(*msg)


class Machine(object):
    def __init__(self, d):
        self.__dict__ = d
        self.jobs = []
        self.apps = {}
        self.original_dict = copy.deepcopy(d)

    def __repr__(self):
        return str(self.__dict__)

    def reset(self):
        self.__dict__ = self.original_dict
        self.jobs = []
        self.apps = {}

    def add_job(self, new_job, only_use_new_machine=False):
        if not new_job.check_interference(self):
            debug('Interference detected')
            return False
            # if not new_job.assigned_machine_id:
            #     print("Failed job: " + str(new_job))
            #     return False
            # raise Exception("This job has already been prescheduled! " + str(new_job))
        # check if resource constraints valid
        for k in ELEMENTS_TO_UPDATE:
            machine_k = getattr(self, k)
            job_k = getattr(new_job, k)
            machine_k_capacity = self.original_dict[k]
            if only_use_new_machine and self.jobs:
                debug('New machine requested but this is not a new machine', only_use_new_machine, self.jobs)
                return False
            if not only_use_new_machine and 'cpu' in k and job_k > CPU_SOFT_LIMIT * machine_k_capacity:
                debug('cpu constraint', job_k, CPU_SOFT_LIMIT * machine_k_capacity)
                return False
            if machine_k - job_k < 0:
                debug('resource constraint')
                return False

                # raise Exception("Attempting to use more than 100% of resource " + k + " .Current Machine State: " + self + " . New job state: " + new_job)
        # actually start mutating
        for k in ELEMENTS_TO_UPDATE:
            machine_k = getattr(self, k)
            job_k = getattr(new_job, k)
            setattr(self, k, machine_k - job_k)
        self.jobs.append(new_job)
        if new_job.app_id in self.apps:
            self.apps[new_job.app_id] += 1
        else:
            self.apps[new_job.app_id] = 1
        debug('new job', new_job)
        return True


class Job(object):
    def __init__(self, d, interference_dict):
        self.__dict__ = d
        self.max_cpu = 0
        for k, v in d.items():
            if "cpu" in k:
                self.max_cpu = max(self.max_cpu, v)
        # self.assigned_machine_id = None
        self.interference = interference_dict

    def check_interference(self, machine):
        if self.app_id in machine.apps:
            cnt_new_app_id = machine.apps[self.app_id] + 1
        else:
            cnt_new_app_id = 1

        for job in machine.jobs:
            interference = job.interference
            for k, v in interference.items():
                if k == self.app_id and v < cnt_new_app_id:
                    debug('interfered')
                    return False

            for k, v in self.interference.items():
                if k == job.app_id and v < machine.apps.get(job.app_id, 0) + 1:
                    debug('interfered2')
                    return False

        return True

    def get_max_cpu(self):
        return self.max_cpu

    def __repr__(self):
        return str(self.__dict__)


def get_csv(short_name, header_lst=None):
    loc = list(filter(lambda x: "csv" in x and short_name in x, os.listdir(DATA_FOLDER)))[0]
    full_loc = os.path.join(os.getcwd(), DATA_FOLDER, loc)
    print("For ", short_name, " Using file at: ", full_loc)
    res = pd.read_csv(full_loc, names=header_lst)
    return res


def pipe_separated_values_into_multiple_cols(df, prefix, original_col_name):
    return pd.concat([df.drop(original_col_name, axis=1),
                      df[original_col_name].str.split("|", expand=True).applymap(lambda x: float(x)).rename(
                          columns=lambda x: prefix + str(x))], axis=1)


def data_parsing_main():
    # instance_id, app_id, id_of_machine this instance is running on
    jobs = get_csv("instance", header_lst=["inst_id", "app_id", "machine_id"])
    # app id 1, app id 2, maximum # of instances of app id 2 that can be on same machine as at least one instance of app id 1
    interference = get_csv("interference", header_lst=["app_id1", "app_id2", "max_app2"])
    # machine id, cpu capacity, memory capacity, disk capacity, P capacity, M capacity, PM capacity
    machine_resources = get_csv("machine_resources",
                                header_lst=["machine_id", "cpu_capacity", "mem_capacity", "disk_capacity", "p_capacity",
                                            "m_capacity", "pm_capacity"])
    # app_id, CPU curve, memory curve, disk req, P resource req, M resource req, PM resource req
    raw_app_resources_1 = get_csv("app_resources",
                                  header_lst=["app_id", "cpu_curve", "mem_curve", "disk", "p", "m", "pm"])
    raw_app_resources_2 = pipe_separated_values_into_multiple_cols(raw_app_resources_1, "cpu_", "cpu_curve")
    app_resources = pipe_separated_values_into_multiple_cols(raw_app_resources_2, "mem_", "mem_curve")

    job_limits = defaultdict(dict)
    for row in interference.to_dict(orient="records"):
        row_appid1 = row["app_id1"]
        row_appid2 = row["app_id2"]
        cap = row["max_app2"]
        if row_appid2 in job_limits:
            job_limits[row_appid2][row_appid1] = cap
        else:
            job_limits[row_appid2] = {row_appid1: cap}
    print(job_limits["app_7845"])
    # check job limit count
    print("LENGTH OF JOB LIMITS ", len(job_limits.keys()))
    print(job_limits)

    print(jobs.columns)
    print(app_resources.columns)
    jobs_with_resources = pd.merge(jobs, app_resources, how='left', on=['app_id'])
    jobs_with_resources_dict = {}
    job_objects_lst = []
    for row in jobs_with_resources.to_dict(orient="records"):
        if row["app_id"] in job_limits:
            row["interference"] = job_limits["app_id"]
        else:
            row["interference"] = {}
        # row["assigned_machine_id"] = None
        jobs_with_resources_dict[row["inst_id"]] = row
        job_objects_lst.append(Job(row, job_limits[row["app_id"]]))

    machine_dict = {}
    machine_objects_lst = []
    for row in machine_resources.to_dict("records"):

        new_item = {}
        new_item["machine_id"] = row["machine_id"]
        for i in range(DURATION):
            i = str(i)
            new_item["cpu_" + i] = row["cpu_capacity"]
            new_item["mem_" + i] = row["mem_capacity"]
        new_item["disk"] = row["disk_capacity"]
        new_item["p"] = row["p_capacity"]
        new_item["m"] = row["m_capacity"]
        new_item["pm"] = row["pm_capacity"]
        machine_dict[row["machine_id"]] = new_item
        machine_objects_lst.append(Machine(new_item))

    return [jobs_with_resources_dict, job_objects_lst, machine_dict, machine_objects_lst]
