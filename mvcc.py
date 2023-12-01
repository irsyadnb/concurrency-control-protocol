from collections import deque
from lib.task import Task
from lib.parser import *

class MVCC:
    def __init__(self, filename: str):
        num_transactions, task_list, transactions = parse(filename=filename,parser_type='mvcc')
        self.task_list: list[Task] = task_list
        self.sequence = deque(self.task_list)
        self.transactions = transactions
        self.version_table = {}
        self.queue = deque()
        self.completed=deque()
        self.version_counter = 0
        self.transactions_timestamp = [i for i in range(num_transactions*2)]
    
    def read(self, task:Task):
        t_num = task.num
        item = task.item
        if (item not in self.version_table.keys()):
            self.version_table[item] = [(t_num, self.transactions_timestamp[t_num],0,self.version_counter)]
            self.completed.append((task, self.transactions_timestamp[t_num],0,self.version_counter))
            print(f"T{(t_num)} read {item} at version 0. Timestamp {item} now: ({self.transactions_timestamp[t_num]}, {0})")
        else:
            idx = len(self.version_table[item])
            max_rts = self.version_table[item][idx-1][1]
            max_wts = self.version_table[item][idx-1][2]
            max_ver = self.version_table[item][idx-1][3]

            if self.transactions_timestamp[t_num] > max_rts:
                self.version_table[item][idx] = (t_num, self.transactions_timestamp[t_num],max_wts,max_ver)
            self.completed.append((task, self.transactions_timestamp[t_num],max_wts,max_ver))
            print(f"T{(t_num)} read {item} at version {max_ver}. Timestamp {item} now: ({self.transactions_timestamp[t_num]}, {max_wts})")
    
    def write(self, task:Task):
        t_num = task.num
        item = task.item
        if (item not in self.version_table.keys()):
            self.version_table[item] = [(t_num, self.transactions_timestamp[t_num],self.transactions_timestamp[t_num],self.transactions_timestamp[t_num])]
            self.completed.append((task, self.transactions_timestamp[t_num],self.transactions_timestamp[t_num],self.transactions_timestamp[t_num]))
            print(f"T{(t_num)} write {item} at version {self.version_counter}. Timestamp {item} now: ({self.transactions_timestamp[t_num]}, {self.transactions_timestamp[t_num]})")
        else:
            idx = len(self.version_table[item])
            max_rts = self.version_table[item][idx-1][1]
            max_wts = self.version_table[item][idx-1][2]
            max_ver = self.version_table[item][idx-1][3]

            if self.transactions_timestamp[t_num] > max_rts:
                self.version_table[item].append((t_num, max_rts,self.transactions_timestamp[t_num],self.transactions_timestamp[t_num]))
            else:
                self.version_table[item].append(self.version_table[item][idx-1])
                print("Rollback")
                self.version_counter-=1
            curr_rts = self.version_table[item][idx][1]
            curr_wts = self.version_table[item][idx][2]
            curr_ver = self.version_table[item][idx][3]
            self.completed.append((task, curr_rts,curr_wts,curr_ver))
            print(f"T{(t_num)} write {item} at version {curr_ver}. Timestamp {item} now: ({curr_rts}, {curr_wts})")
    
    def run(self):
        while len(self.sequence) > 0:
            task = self.sequence.popleft()
            if task.type == 'R':
                self.read(task)
            elif task.type == 'W':
                self.write(task)
            elif task.type == 'C':
                print("T" + str(task.num) + " commit.")
            else:
                print("Invalid action")
                exit(1)
            self.completed.append(task)

        print("Final timestamp:")
        for i in range(len(self.transactions_timestamp)):
            print("T" + str(i) + ": " + str(self.transactions_timestamp[i]))
        print()

    def printTaskSequence(self, tasks):
        for task in tasks:
            print(str(task[0]), end=" ")
        print()

if __name__ == "__main__":
  mvcc = MVCC('test1.txt')
  mvcc.run()
