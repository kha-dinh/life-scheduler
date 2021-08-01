import random
from operator import attrgetter  
import copy

class Task:
    # There are 6 characteristics of jobs (taken from Scheduling Algorithms book):
    # 1. preemption,
    # 2. precedence relations,
    # 3. release date,
    # 4. restriction on processing time (time limit?) / number of operation ,
    # 5. deadline
    # 6. batching (is it needed?)
    def __init__(self,id, name, deadline, arrival_time, estimated_time=100, priority=1, prerequisite=[], preemptable=True, min_quantum=20):
        self.name = name
        self.id = id
        self.priority = priority
        self.remaining_time = estimated_time
        self.estimated_time = estimated_time
        self.preemptable = preemptable
        self.arrival_time = arrival_time
        self.deadline = deadline
    
        self.min_quantum = min_quantum # Minimum scheduling granularity
        # TODO
        self.prerequisite = prerequisite 

    def finished(self):
        return self.remaining_time <= 0


class ScheduledTasks:
    # Contains the scheduled results from scheduler
    # Should have: timestamps, task
    # Should be algorithm-agnostic
    def __init__(self):
        self.scheduled_tasks = {}
        self.curr_clock = 0
        self.waiting_time = 0
        self.lateness = 0
        self.deadline_missed = []

    def add_task(self,task, process_time):
        #TODO: should there be a sanity check?
        if self.curr_clock < task.arrival_time:
            # Update clock to match arrival time
            self.curr_clock = max(task.arrival_time, self.curr_clock)

        # TODO: Should it be copied or referenced? Copy for now
        copied_task = copy.deepcopy(task)
        copied_task.process_time = process_time

        self.scheduled_tasks[self.curr_clock] = copied_task
        # TODO: each task should have its own scheduled time / departure time
        self.curr_clock += process_time
        self.update_statistics(task)

    def update_statistics(self, task):
        # Update statistics based on scheduled task
        if task.finished():
            # FIX: The use of curr_clock is not clean?
            # Waiting time (or flow time): from arrival time -> finish task
            self.waiting_time += (self.curr_clock - task.arrival_time)
            # Lateness: deviation from deadline. 
            # Negative lateness is earliness, positve lateness is tardiness
            lateness = (task.deadline - self.curr_clock)
            self.lateness += lateness
            if lateness < 0:
                self.deadline_missed.append((task, self.curr_clock))
      

    def finalize(self):
        num_unique_tasks = self.get_num_unique_tasks()
        self.waiting_time /=  num_unique_tasks
        self.lateness /= num_unique_tasks

    def get_num_unique_tasks(self):
        unique_tasks = set([task.id for _, task in self.scheduled_tasks.items()])
        return len(unique_tasks)
#        for _ , task in self.scheduled_tasks.items():
            

    def get_clock(self):
        return self.curr_clock

    def print(self):
        for time, task in self.scheduled_tasks.items():
            print(f'[{time}]: {task.name}, processing time: {task.process_time} (remaining time: {task.remaining_time})')

    def print_statistics(self):
        print(f'Average waiting time: {self.waiting_time}') 
        print(f'Average lateness : {self.lateness}') 

        print(f'Deadlines missed:') 
        for (task, time) in self.deadline_missed:
            print(f'[{time}], {task.name}, (deadline: {task.deadline})') 





class Scheduler:
    def __init__(self, algorithm="FCFS"):
        self.unscheduled_task_list = []
        self.algorithm = algorithm
        self.rr_index = -1

    def schedule(self, task_list):
        # Copy so that the original list is not affected
        self.unscheduled_task_list = copy.deepcopy(task_list)
        num_tasks = len(self.unscheduled_task_list)
        # Sort the list first
        self.sort_task_list()
        scheduled_tasks = ScheduledTasks()
        while self.unscheduled_task_list:
            task = self.get_next_task(scheduled_tasks.get_clock())         

            # self.scheduled_routine[clock] = task
            # clock += task.remaining_time
            # TODO: doesn't look good
            if task.preemptable and self.is_preemtive():
                # Schedule in quantum granularity
                process_time = min(task.min_quantum, task.remaining_time)
                task.remaining_time -=  process_time

                scheduled_tasks.add_task(task, process_time)
                # print(f'[{clock}]: {task.name}, Process time: {process_time} (Remaining time = {task.remaining_time - process_time})')
                # clock += process_time
                if task.remaining_time <= 0:
                #     waiting_time += (clock - task.arrival_time)
                    self.unscheduled_task_list.remove(task)
            else:
                process_time = task.remaining_time
                task.remaining_time = 0

                # print(f'[{clock}]: {task.name}, Process time: {process_time} (Non-preempted)')
                scheduled_tasks.add_task(task, process_time)
                # clock += process_time
                # waiting_time += (clock - task.arrival_time)
                self.unscheduled_task_list.remove(task)

        # waiting_time = waiting_time / num_tasks
        # print(f'Average Waiting Time: {waiting_time}')
        scheduled_tasks.finalize()
        return scheduled_tasks  

    def is_preemtive(self):
        # TODO: Each task should also have non-preemtive option
        return self.algorithm == "SRTF" or self.algorithm == "RR" or self.algorithm == "LRTF"

    def sort_task_list(self):
        # Sort the list based on algorithm
        if self.algorithm == "FCFS" or self.algorithm == "RR":
            self.unscheduled_task_list.sort(key=lambda t: t.arrival_time )
        elif self.algorithm == "EDD":
            self.unscheduled_task_list.sort(key=lambda t: t.deadline)
        elif self.algorithm == "SPT" or self.algorithm == "SJF":
            self.unscheduled_task_list.sort(key=lambda t: t.estimated_time)
        elif self.algorithm == "SRTF":
            self.unscheduled_task_list.sort(key=lambda t: t.remaining_time)
        elif self.algorithm == "LRTF":
            self.unscheduled_task_list.sort(key=lambda t: t.remaining_time, reverse=True)


    def get_next_task(self, curr_clock):
        # Get next task based on the algorithm
        # TODO: should be able to handle non-preemtive tasks
        # TODO: There should be a better way without sorting every time...
        # TODO: Upgrade to match case for  python 3.10
        
        if self.algorithm == "LRTF":
            # The order change at every iteration
            self.sort_task_list()

        next_index = 0 
        if self.algorithm == "RR":
            next_index = (self.rr_index + 1) % len(self.unscheduled_task_list)
        task = self.unscheduled_task_list[next_index]
            
        while task.arrival_time > curr_clock:                
            # Get another task if it havent arrived yet
            # FIX: Not clean
            next_index = (next_index + 1) % len(self.unscheduled_task_list)
            task = self.unscheduled_task_list[next_index]
            if next_index == len(self.unscheduled_task_list) - 1:
                curr_clock = min(self.unscheduled_task_list, key=attrgetter('arrival_time')).arrival_time

        if self.algorithm == "RR":
            # Remember the index
            self.rr_index = next_index
        return task
            
        # Shortest remaining time first is a preemtive mode of SJF
        # Earliest due date
        # self.task_list.sort(key=lambda t: t.deadline)

    # def get_statistics(self, verbose = False):
    #     # I guess flow time is the average wating time from arrival to departure
    #     flow_time = 0
    #     lateness = 0
    #     clock = 0
    #     deadline_missed = 0
    #     # trace = []
    #     for task in self.task_list:
    #         # Handle arrival time
    #         if clock < task.arrival_time:
    #             clock = task.arrival_time

    #         clock += task.estimated_time # This is the time the job is done
    #         lateness += (task.deadline - clock)
    #         if (lateness < 0):
    #             # print(f'{task.name} will miss deadline')
    #             deadline_missed += 1
    #         flow_time += (clock - task.arrival_time)
    #         # trace.append(f'{clock}: {task.name}')

    #     flow_time = flow_time / len(self.task_list)
    #     lateness = lateness / len(self.task_list)
    #     print(f'Algorithm: {self.algorithm}, Average waiting time: {flow_time}, Average lateness: {lateness} Deadline missed: {deadline_missed}')
    #     # if verbose:
    #         # print(" ".join(trace))


    def print(self):
        print("Dumping all of the tasks")
        [print(f'Name: {t.name}, Arrival time: {t.arrival_time}, Estimated time: {t.estimated_time},  Remaining time: {t.remaining_time}, Deadline: {t.deadline}')
         for t in self.unscheduled_task_list]

def test():
    random.seed(69)
    task_list = []
    arrival_time = 0
    for i in range(10):
        arrival_time += random.randint(1,5)
        task_list.append(Task(name="Task_" + str(i),id = i, arrival_time=arrival_time, estimated_time=random.randint(10, 100), deadline=arrival_time + random.randint(100, 1000)))
    
    RR = Scheduler("RR")
    FCFS = Scheduler("FCFS")
    EDD = Scheduler("EDD")
    # RR.print()
    # FCFS.print()
    # FCFS_scheduled_tasks = FCFS.schedule(task_list)
    # FCFS_scheduled_tasks.print()
    # RR.print()
    scheduled_tasks = RR.schedule(task_list)
    scheduled_tasks.print()
    scheduled_tasks.print_statistics()

    scheduled_tasks = FCFS.schedule(task_list)
    scheduled_tasks.print()
    scheduled_tasks.print_statistics()


    scheduled_tasks = EDD.schedule(task_list)
    scheduled_tasks.print()
    scheduled_tasks.print_statistics()
    # RR.print()
    # task_list.print()
    # task_list.get_statistics(verbose=True)

    # task_list.schedule("EDD")
    # task_list.print()
    # task_list.get_statistics()

    # task_list.schedule("SJF")
    # task_list.print()
    # task_list.get_statistics()
test()
