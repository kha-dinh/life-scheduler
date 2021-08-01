# -*- coding: utf-8 -*-

from .context import sample, Task, TaskList

import random
import unittest


class BasicTestSuite(unittest.TestCase):
    """Basic test cases."""

    def test_absolute_truth_and_meaning(self):
        assert True
    def test_scheduling(self):
        random.seed(69)
        task_list = []
        arrival_time = 0
        for i in range(10):
            arrival_time += random.randint(1,5)
            task_list.append(Task(name="Task_" + str(i), arrival_time=arrival_time, estimated_time=random.randint(10, 100), deadline=arrival_time + random.randint(100, 1000)))

        RR = TaskList(task_list, "RR")
        FCFS = TaskList(task_list, "FCFS")
        # RR.print()
        # FCFS.print()
        # FCFS.schedule()
        # RR.print()
        RR.schedule()

if __name__ == '__main__':
    unittest.main()