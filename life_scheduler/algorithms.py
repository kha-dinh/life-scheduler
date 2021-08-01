import random
from operator import attrgetter
import copy
from re import I
from queue import Queue
import re


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
        self.preemptible = preemptible
        self.arrival_time = arrival_time
        self.deadline = deadline
        self.min_quantum = min_quantum  # Minimum scheduling granularity
        self.quota = min_quantum

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
        self.waiting_time = 0
        self.makespan = 0  # a.k.a turnaround time
        self.lateness = 0
        self.deadline_missed = 0

    def add_task(self, task, clock, process_time):
        # Add a task into the queue
        # TODO: should there be a sanity check?
        # if self.curr_clock < task.arrival_time:
        # Update clock to match arrival time
        # self.curr_clock = max(task.arrival_time, self.curr_clock)

        # TODO: Should it be copied or referenced? Copy for now
        copied_task = copy.deepcopy(task)
        copied_task.process_time = process_time

        self.scheduled_tasks[clock] = copied_task
        # TODO: each task should have its own scheduled time / departure time
        self.update_statistics(task, clock)

    def update_statistics(self, task, clock):
        # Update statistics based on scheduled task
        if task.finished():
            # FIX: The use of curr_clock is not clean?
            # Waiting time (or flow time): from arrival time -> finish task
            self.makespan += (clock - task.arrival_time)
            # Lateness: deviation from deadline.
            # Negative lateness is earliness, positve lateness is tardiness
            lateness = (task.deadline - clock)
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

        # for (task, time) in self.deadline_missed:
        #     print(f'[{time}], {task.name}, (deadline: {task.deadline})')


class Scheduler:
    def __init__(self, preemptive, verbose=False):
        self.preemptive = preemptive
        self.not_arrived = []
        self.pending = []
        self.completed = []
        self.active = None
        self.unscheduled_task_list = []
        self.verbose = verbose

    def schedule(self, task_list):
        # https://nicomedes.assistedcoding.eu/#/app/os/process_scheduling ref
        # Copy so that the original list is not affected
        # Maybe we shouldn't handle it here...
        self.unscheduled_task_list = copy.deepcopy(task_list)

        # [self.not_arrived.put(task) for task in self.unscheduled_task_list]
        self.not_arrived = copy.copy(self.unscheduled_task_list)
        self.not_arrived.sort(key=lambda t: t.arrival_time)
        # Nothing arrived now

        # Shallow copy (different order, same references)
        # all_task_list = copy.copy(self.unscheduled_task_list)

        self.clock = 0
        # Sort the list first
        # self.sort_task_list()
        # scheduled_tasks = ScheduledTasks()
        # Begin scheduling
        next_tick = 0
        while self.unscheduled_task_list:
            next_tick = self.process_tick(next_tick)
            # self.active.quota -= next_tick
            # self.active.remaining_time -= next_tick
            # clock += next_tick

            # self.process_tick(clock)

            # task = self.get_next_task(clock)

            # if task.preemptable and self.preemtive:
            #     # Schedule in quantum granularity
            #     # TODO: preemption should be handled here
            #     # It should also check if next arrival task have smaller time or not ...
            #     # time_until_next_event_arrive = curr_clock -
            #     all_task_list.remove(task)
            #     next_arrival =  min(all_task_list, key=attrgetter('arrival_time')).arrival_time - clock
            #     all_task_list.append(task)
            #     if next_arrival > 0:
            #         process_time = min(task.min_quantum, task.remaining_time, next_arrival)
            #     else:
            #         process_time = min(task.min_quantum, task.remaining_time)
            # else:
            #     process_time = task.remaining_time
            # # task is scheduled
            # task.remaining_time -= process_time
            # scheduled_tasks.add_task(task, clock, process_time)

            # # Finally, increment clock
            # clock += process_time
            # if task.remaining_time <= 0:
            #     self.unscheduled_task_list.remove(task)

        # scheduled_tasks.finalize()
        # return scheduled_tasks
        
    def print_verbose(self, format):
        if self.verbose:
            print(format)

    def process_tick(self, tick):
        self.clock += tick
        self.print_verbose(f'[Tick {self.clock}]:')
        if self.active:
            running_task = self.active
            if self.preemptive and running_task.preemptible:
                running_task.quota -= tick
            running_task.remaining_time -= tick
            self.print_verbose(
                f'Execute Process {running_task.name}: {running_task.estimated_time-running_task.remaining_time}/{running_task.estimated_time}')
            if self.active.remaining_time <= 0:
                self.print_verbose(f'Process {running_task.name} retired')
                self.unscheduled_task_list.remove(running_task)
                self.completed.append(running_task)
                self.active = None

        # Move not_arrived -> arrived
        while True:
            if self.not_arrived:
                arriving_task = self.not_arrived[0]
                if arriving_task.arrival_time <= self.clock:
                    self.print_verbose(
                        f'Process {arriving_task.name} enter pending queue')
                    self.not_arrived.remove(arriving_task)
                    self.pending.append(arriving_task)
                else:
                    break
            else:
                arriving_task = None
                break

        # Move pending -> active
        if self.pending:
            task = self.get_next_task()
            if not self.active:
                # Idling
                self.print_verbose(
                    f'Process {task.name} moved pending -> active')
                self.active = task
                self.active.quota = self.active.min_quantum
                self.pending.remove(task)
            else:
                running_task = self.active
                # preempt task only when it used its quota
                if self.preemptive and running_task.preemptible and running_task.quota <= 0:
                    self.print_verbose(
                        f'Preempted {running_task.name}, process {task.name} moved pending -> active')
                    # preempt:
                    self.active = task
                    self.pending.remove(task)
                    # Refresh quota
                    running_task.quota = running_task.min_quantum
                    self.pending.append(running_task)

       # Finally, return next tick?
        if self.active:
            if self.preemptive and self.active.preemptible:
                if arriving_task:
                    next_tick = min(self.active.quota, self.active.remaining_time, arriving_task.arrival_time - self.clock)
                elif self.pending:
                    next_tick = min(self.active.quota, self.active.remaining_time)
                else :
                    next_tick = self.active.remaining_time
            else:
                # Non-preemptible
                if arriving_task:
                    next_tick = min(self.active.remaining_time, arriving_task.arrival_time - self.clock)
                else:
                    next_tick = self.active.remaining_time
        else:
            next_tick = 0

        return next_tick
    # def get_task(self,clock,index):

    def try_get_task(self, clock, index):
        # Try to get the task that arrive before current time
        task = self.unscheduled_task_list[index]
        # Scan for next candidate
        # num_try = 1
        if task.arrival_time > clock:
            for i in range(1, len(self.unscheduled_task_list)):
                # while task.arrival_time >= clock:
                index = (index + 1) % len(self.unscheduled_task_list)
                task = self.unscheduled_task_list[index]
                if task.arrival_time < clock:
                    break
                if i == len(self.unscheduled_task_list):
                    task = None
                    break

        # Failed to  get a candidate, get the minimim candidate
        if task is None:
            task = min(self.unscheduled_task_list,
                       key=attrgetter('arrival_time'))
            # update arrival time
            clock = task.arrival_time
        return task

    # def sort_task_list(self):
        # self.pending.sort(key=lambda t: t.arrival_time)

    def get_next_task(self):
        # Get next task based on the algorithm
        self.sort_task_list()
        # task = self.try_get_task(clock, 0)
        task = self.pending[0]
        return task
        # Shortest remaining time first is a preemtive mode of SJF
        # Earliest due date
        # self.task_list.sort(key=lambda t: t.deadline)


class RRScheduler(Scheduler):
    def __init__(self, preemtive=True):
        Scheduler.__init__(self, preemtive)
        self.rr_index = -1

    def sort_task_list(self):
        self.pending.sort(key=lambda t: t.arrival_time)

    def get_next_task(self, clock):
        self.rr_index = (self.rr_index + 1) % len(self.pending)
        # task = self.try_get_task(clock, self.rr_index)
        task = self.pending[self.rr_index]
        return task


class SJFScheduler(Scheduler):
    def __init__(self, preemtive=False):
        Scheduler.__init__(self, preemtive)

    def sort_task_list(self):
        self.pending.sort(key=lambda t: t.estimated_time)


class LJFScheduler(Scheduler):
    def __init__(self, preemtive=False):
        Scheduler.__init__(self, preemtive)

    def sort_task_list(self):
        self.pending.sort(key=lambda t: t.estimated_time, reverse=True)


class FCFSScheduler(Scheduler):
    def __init__(self, preemtive=False, verbose=False):
        Scheduler.__init__(self, preemtive, verbose)

    def sort_task_list(self):
        self.pending.sort(key=lambda t: t.arrival_time)


class EDFScheduler(Scheduler):
    # Earilest deadline First
    def __init__(self, preemtive=False):
        Scheduler.__init__(self, preemtive)

    def sort_task_list(self):
        self.pending.sort(key=lambda t: t.deadline)


def get_scheduler_from_string(name, preemptive=False):
    # NOTE: Match case is so much cleaner here
    scheduler = None
    if name == "RR":
        scheduler = RRScheduler(preemptive)
    if name == "FCFS":
        scheduler = FCFSScheduler(preemptive, verbose=True)
    if name == "EDF":
        scheduler = EDFScheduler(preemptive)
    if name == "SJF":
        scheduler = SJFScheduler(preemptive)
    if name == "LJF":
        scheduler = LJFScheduler(preemptive)

    assert scheduler is not None, "Unknown scheduler name"
    return scheduler


def test_scheduler(algorithm, task_list, preemptive=False):
    print(f'Testing scheduler with {algorithm} algorithm',
          '(preemptive)' if preemptive == True else '(non-preemptive)')
    # scheduler = Scheduler(algorithm)
    scheduler = get_scheduler_from_string(algorithm, preemptive)
    scheduled_tasks = scheduler.schedule(task_list)
    # scheduled_tasks.print()
    # scheduled_tasks.print_statistics()


def test_all_schedulers(task_list):
    all_schedulers = ["FCFS", "EDF", "RR", "SJF", "LJF"]
    for sched in all_schedulers:
        test_scheduler(sched, task_list, False)
        test_scheduler(sched, task_list, True)


def test_schedulers_random_tasks(num_tasks=100):
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
    task_list.append(Task(name="A", id=0, arrival_time=0, estimated_time=3,
                     deadline=10, min_quantum=2, priority=1, preemptible=True))
    task_list.append(Task(name="B", id=1, arrival_time=2, estimated_time=6,
                     deadline=11, min_quantum=2, priority=2, preemptible=True))
    task_list.append(Task(name="C", id=2, arrival_time=3, estimated_time=10,
                     deadline=12, min_quantum=2, priority=2, preemptible=True))
    task_list.append(Task(name="D", id=3, arrival_time=7, estimated_time=1,
                     deadline=13, min_quantum=2, priority=3, preemptible=True))
    task_list.append(Task(name="E", id=4, arrival_time=8, estimated_time=5,
                     deadline=14, min_quantum=2, priority=1, preemptible=True))
    task_list.append(Task(name="F", id=5, arrival_time=15, estimated_time=2,
                     deadline=14, min_quantum=2, priority=1, preemptible=True))
    task_list.append(Task(name="G", id=6, arrival_time=25, estimated_time=7,
                     deadline=14, min_quantum=2, priority=1, preemptible=True))

    # test_all_schedulers(task_list)

    # test_scheduler("FCFS", task_list, preemptive=True)
    test_scheduler("FCFS", task_list, preemptive=False)
    # test_scheduler("EDF",task_list, preemptive = True)
    # test_scheduler("EDF",task_list, preemptive = False)


test_schedulers_basic()
# test_schedulers_random_tasks(1000)
