import random
from operator import attrgetter
import copy
from re import I
class Task:
    # There are 6 characteristics of jobs (taken from Scheduling Algorithms book):
    # 1. preemption,
    # 2. precedence relations,
    # 3. release date,
    # 4. restriction on processing time (time limit?) / number of operation ,
    # 5. deadline
    # 6. batching (is it needed?)
    def __init__(self, id, name, deadline, arrival_time, estimated_time=-1, priority=1, prerequisite=[], preemptible=True, min_quantum=20):
        self.name = name
        self.id = id
        self.priority = priority
        self.remaining_time = estimated_time
        self.estimated_time = estimated_time
        self.preemptable = preemptible
        self.arrival_time = arrival_time
        self.deadline = deadline
        self.min_quantum = min_quantum  # Minimum scheduling granularity

        # TODO
        self.prerequisite = prerequisite

    def finished(self):
        return self.remaining_time <= 0


class ScheduledTasks:
    # Contains the scheduled results from scheduler
    # Should have: timestamps, task
    # Should be algorithm-agnostic
    # TODO: should also be clock-agnostic...
    def __init__(self):
        self.scheduled_tasks = {}
        self.curr_clock = 0
        self.waiting_time = 0
        self.makespan = 0  # a.k.a turnaround time
        self.lateness = 0
        self.deadline_missed = 0

    def add_task(self, task, process_time):
        # Add a task into the queue
        # TODO: should there be a sanity check?
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
            self.makespan += (self.curr_clock - task.arrival_time)
            # Lateness: deviation from deadline.
            # Negative lateness is earliness, positve lateness is tardiness
            lateness = (task.deadline - self.curr_clock)
            self.lateness += lateness
            if lateness < 0:
                # self.deadline_missed.append((task, self.curr_clock))
                self.deadline_missed += 1

    def finalize(self):
        num_unique_tasks = self.get_num_unique_tasks()
        self.makespan /= num_unique_tasks
        self.lateness /= num_unique_tasks

        previous_task = None
        # Use a remove list to keep the length consistent
        remove_list = []

        # Merge entries
        for time, task in self.scheduled_tasks.items():
            if previous_task is not None: 
                if task.id == previous_task.id:
                    previous_task.process_time += task.process_time
                    previous_task.remaining_time = task.remaining_time
                    remove_list.append(time)
                else:
                    previous_task = task
            else: 
                previous_task = task
            
        [self.scheduled_tasks.pop(item) for item in remove_list]
            
            

    def get_num_unique_tasks(self):
        unique_tasks = set(
            [task.id for _, task in self.scheduled_tasks.items()])
        return len(unique_tasks)
#        for _ , task in self.scheduled_tasks.items():

    def print(self):
        for time, task in self.scheduled_tasks.items():
            print(f'[{time} -> {time + task.process_time}]: {task.name} (arrival time: {task.arrival_time}, remaining time: {task.remaining_time}, deadline: {task.deadline})')

    def print_statistics(self):
        print(f'Average makespan: {self.makespan}')
        print(f'Average lateness : {self.lateness}')
        print(f'Deadline missed: {self.deadline_missed}')
    

        # print(f'Deadlines missed:')
        # for (task, time) in self.deadline_missed:
        #     print(f'[{time}], {task.name}, (deadline: {task.deadline})')


class Scheduler:
    # TODO: Maybe this should also be algorithm-agnostic
    def __init__(self, preemtive):
        self.preemtive = preemtive

    def schedule(self, task_list):
        # Copy so that the original list is not affected
        # Maybe we shouldn't handle it here...
        self.unscheduled_task_list = copy.deepcopy(task_list)
        # Shallow copy (different order, same references)
        all_task_list = copy.copy(self.unscheduled_task_list)
        # self.sorted_unscheduled_task_list.sort(key=lambda t: t.arrival_time)

        # Sort the list first
        self.sort_task_list()
        scheduled_tasks = ScheduledTasks()
        while self.unscheduled_task_list:
            task = self.get_next_task(scheduled_tasks.curr_clock)

            if task.preemptable and self.preemtive:
                # Schedule in quantum granularity
                # TODO: preemption should be added here
                # It should also check if next arrival task have smaller time or not ...
                # time_until_next_event_arrive = curr_clock -  
                all_task_list.remove(task)
                next_arrival =  min(all_task_list, key=attrgetter('arrival_time')).arrival_time - scheduled_tasks.curr_clock
                all_task_list.append(task)
                if next_arrival > 0:
                    process_time = min(task.min_quantum, task.remaining_time, next_arrival)
                else:
                    process_time = min(task.min_quantum, task.remaining_time)
            else:
                process_time = task.remaining_time
            # task is scheduled
            task.remaining_time -= process_time
            scheduled_tasks.add_task(task, process_time)
            if task.remaining_time <= 0:
                self.unscheduled_task_list.remove(task)

        scheduled_tasks.finalize()
        return scheduled_tasks

    def try_get_task(self, curr_clock, index):
        # Try to get the task that arrive before current time
        task = self.unscheduled_task_list[index]
        # Scan for next candidate
        num_try = 1
        while task.arrival_time > curr_clock:
            index = (index + 1) % len(self.unscheduled_task_list)
            task = self.unscheduled_task_list[index]
            if task.arrival_time <= curr_clock:
                break
            num_try += 1
            if num_try == len(self.unscheduled_task_list):
                task = None
                break

        # Failed to  get a candidate, get the minimim candidate
        if task is None:
            task = min(self.unscheduled_task_list,
                       key=attrgetter('arrival_time'))
            # update arrival time
            curr_clock = task.arrival_time
        return task

    def get_next_task(self, curr_clock):
        # Get next task based on the algorithm
        self.sort_task_list()
        task = self.try_get_task(curr_clock, 0)
        return task
        # Shortest remaining time first is a preemtive mode of SJF
        # Earliest due date
        # self.task_list.sort(key=lambda t: t.deadline)


class RRScheduler(Scheduler):
    def __init__(self, preemtive=True):
        Scheduler.__init__(self, preemtive)
        self.rr_index = -1

    def sort_task_list(self):
        self.unscheduled_task_list.sort(key=lambda t: t.arrival_time)

    def get_next_task(self, curr_clock):
        self.rr_index = (self.rr_index + 1) % len(self.unscheduled_task_list)
        task = self.try_get_task(curr_clock, self.rr_index)
        # Remember the index
        return task


class SJFScheduler(Scheduler):
    # TODO: Support preemption
    def __init__(self, preemtive=False):
        Scheduler.__init__(self, preemtive)

    def sort_task_list(self):
        self.unscheduled_task_list.sort(key=lambda t: t.estimated_time)


class FCFSScheduler(Scheduler):
    def __init__(self, preemtive=False):
        Scheduler.__init__(self, preemtive)

    def sort_task_list(self):
        self.unscheduled_task_list.sort(key=lambda t: t.arrival_time)


class EDFScheduler(Scheduler):
    # Earilest deadline First
    def __init__(self, preemtive=False):
        Scheduler.__init__(self, preemtive)

    def sort_task_list(self):
        self.unscheduled_task_list.sort(key=lambda t: t.deadline)


def get_scheduler_from_string(name, preemptive=False):
    # NOTE: Match case is so much cleaner here
    scheduler = None
    if name == "RR":
        scheduler = RRScheduler(preemptive)
    if name == "FCFS":
        scheduler = FCFSScheduler(preemptive)
    if name == "EDF":
        scheduler = EDFScheduler(preemptive)
    if name == "SJF":
        scheduler = SJFScheduler(preemptive)

    assert scheduler is not None, "Unknown scheduler name"
    return scheduler


def test_scheduler(algorithm, task_list, preemptive = False):
    print(f'Testing scheduler with {algorithm} algorithm', '(preemptive)' if preemptive == True else '(non-preemptive)')
    # scheduler = Scheduler(algorithm)
    scheduler = get_scheduler_from_string(algorithm, preemptive)
    scheduled_tasks = scheduler.schedule(task_list)
    scheduled_tasks.print()
    scheduled_tasks.print_statistics()

def test_all_schedulers(task_list):
    all_schedulers = ["FCFS", "EDF", "RR", "SJF"]
    for sched in all_schedulers:
        test_scheduler(sched, task_list)

def test_schedulers_random_tasks(num_tasks = 100):
    print("------ Random tasks test -------")
    print(f'Generating {num_tasks} tasks')
    random.seed(69)
    task_list = []
    arrival_time = 0
    for i in range(num_tasks):
        arrival_time += random.randint(0, 10)
        task_list.append(Task(name="Task_" + str(i), id=i, arrival_time=arrival_time,
                         estimated_time=random.randint(10, 100), deadline=arrival_time + random.randint(10, 1000)))

    test_all_schedulers(task_list)

def test_schedulers_basic():
    print("Basic test")
    task_list = []
    task_list.append(Task(name="Task_1", id=0, arrival_time=2, estimated_time=6,
                     deadline=10, min_quantum=2, priority=1, preemptible=True))
    task_list.append(Task(name="Task_2", id=1, arrival_time=5, estimated_time=3,
                     deadline=11, min_quantum=2, priority=2, preemptible=True))
    task_list.append(Task(name="Task_3", id=2, arrival_time=1, estimated_time=8,
                     deadline=12, min_quantum=2, priority=2, preemptible=True))
    task_list.append(Task(name="Task_4", id=3, arrival_time=0, estimated_time=3,
                     deadline=13, min_quantum=2, priority=3, preemptible=True))
    task_list.append(Task(name="Task_5", id=4, arrival_time=4, estimated_time=4,
                     deadline=14, min_quantum=2, priority=1, preemptible=True))

    # test_all_schedulers(task_list)
    test_scheduler("SJF",task_list, preemptive = True)

test_schedulers_basic()
# test_schedulers_random_tasks(1000)
