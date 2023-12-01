from collections import deque
from lib.task import Task
from lib.parser import parse

class TwoPL:
    def __init__(self, filename: str):
        num_transactions, task_list, transactions = parse(filename=filename,parser_type='twopl')
        self.task_list: list[Task] = task_list
        self.sequence = deque(self.task_list)
        self.num_transactions = num_transactions
        self.transactions = transactions
        self.lock_table = {}
        self.queue = deque()
        self.completed=deque()
        self.incomplete_schedule= {}
        self.transaction_table = {}
        self.res = [] # print result
    
    def getNumInLockTable(self, item: str, num: int) -> int:
        if item in self.lock_table:
            matching_tuples = [i for i, (t_num, _) in enumerate(self.lock_table[item]) if t_num == num]
            if matching_tuples:
                return matching_tuples[0]
            else:
                return -1
        return -1
    
    def isAllShareLock(self, item: str) -> bool:
        if item in self.lock_table:
            for i in self.lock_table[item]:
                if i[1] != 'S':
                    return False
        return True
    
    def addTaskToQueueByNum(self, t_num:int):
        idx = 0
        while idx < len(self.sequence):
            if self.sequence[idx].num == t_num:
                self.queue.append(self.sequence[idx])
            idx+=1

        self.sequence = deque(task for task in self.sequence if task.num != t_num)

    # True if lock granted, False if not
    def share_lock(self, task: Task) -> bool:
        item = task.item
        t_num = task.num
        if item not in self.lock_table:
            self.lock_table[item] =[(t_num, 'S')]
            self.completed.append(Task(type="SL", num=t_num, item=item))
            print("[+] Grant Share Lock to T" + str(t_num) + " on " + item)
            return True
        elif self.isAllShareLock(item):
            idx = self.getNumInLockTable(item, t_num)
            if(idx != -1):
                print("[.] T" + str(t_num) + " already has Lock on " + item)
                return True
            else:
                self.lock_table[item].append((t_num, 'S'))
                self.completed.append(Task(type="SL", num=t_num, item=item))
                print("[+] Grant Share Lock to T" + str(t_num) + " on " + item)
                return True
        else:
            # Wait Die Mechanism
            if self.lock_table[item][0][0] > t_num:
                print("[.] T" + str(t_num) + " is waiting Lock on " + item)
                self.queue.append(task)
                self.addTaskToQueueByNum(t_num)
                return False
            else:
                print("[!] Aborting T" + str(t_num))
                self.rollback(task)
                self.completed.append(Task(type="A", num=t_num, item=''))
                return False
        
    def exclusive_lock(self, task: Task)->bool:
        item = task.item
        t_num = task.num
        if item not in self.lock_table:
            self.lock_table[item] = [(t_num, 'X')]
            self.completed.append(Task(type="XL", num=t_num, item=item))
            print("[+] Grant Exclusive Lock to T" + str(t_num) + " on " + item)
            return True
        else:
            idx = self.getNumInLockTable(item, t_num)
            if idx != -1:
                if self.lock_table[item][idx][1]=="X":
                    print("[.] T" + str(t_num) + " already has Lock on " + item)
                    return True
                else:
                    self.lock_table[item].pop(idx)
                    self.lock_table[item].append((t_num, 'X'))
                    self.completed.append(Task(type="UPL", num=t_num, item=item))
                    print("[+] Upgrade Lock to T" + str(t_num) + " on " + item)
                    return True
                
            # Wait Die Mechanism
            if self.lock_table[item][0][0] > t_num:
                self.queue.append(task)
                self.addTaskToQueueByNum(t_num)
                print("[.] T" + str(t_num) + " is waiting Lock on " + item)
                return False
            else:
                print("[!] Aborting T" + str(t_num) )
                self.rollback(task)
                self.completed.append(Task(type="A", num=t_num, item=''))
                return False
        
    def unlock(self, t_num: int):
        # unlock all keys that hold by t_num, if the item is empty after unlock, delete the item
        key_lists = list(self.lock_table.keys())        
        for i in range(len(key_lists)):
            key = key_lists[i]
            for j in range(len(self.lock_table[key])):
                if self.lock_table[key][j][0] == t_num:
                    self.lock_table[key].pop(j)
                    self.completed.append(Task(type="UL", num=t_num, item=key))
                if not self.lock_table[key]:
                    del self.lock_table[key]

    def clearQbyNum(self, t_num: int):
        self.queue = deque(task for task in self.queue if task.num != t_num)

    def rollback(self, task: Task):
        t_num = task.num
        idx = 0
        while idx < len(self.completed):
            if self.completed[idx].num == t_num:
                if self.completed[idx].type == 'R' or self.completed[idx].type == 'W':
                    self.queue.append(self.completed[idx])  
            idx += 1

        self.completed = deque(task for task in self.completed if task.num != t_num)
        self.clearQbyNum(t_num)
        self.queue.append(task)
        self.addTaskToQueueByNum(t_num)
        self.unlock(t_num)

    def printTaskSequence(self, tasks: list[Task]):
        for task in tasks:
            print(str(task), end=" ")
        print()

    def checkIncompleteSchedule(self, t_number)->bool:
        if t_number in self.incomplete_schedule.keys():
            return self.incomplete_schedule[t_number]
        return False

    def start(self):
        while self.sequence or self.queue:
            if len(self.queue) > 0:
                if self.queue[0].item not in self.lock_table.keys():
                    self.sequence.appendleft(self.queue.popleft())
                    self.incomplete_schedule[self.sequence[0].num] = False

            # Move queue to sequence if sequence is empty
            if(not self.sequence):
                self.sequence = self.queue
            
            current_task = self.sequence.popleft()
            print ("Current Task: "+ str(current_task))
            if (self.checkIncompleteSchedule(current_task.num)):
                continue

            current_transaction = list(filter(lambda obj: obj.num == current_task.num, self.transactions))[0]
            
            if(current_task.type == 'C'):
                self.completed.append(current_task)
                self.unlock(current_task.num)
                print(f"[-] Committing T{current_task.num}, Releasing All Held Locks")
                current_transaction.commit()
            
            elif(current_task.type == 'R'):
                shared_lock = self.share_lock(current_task)
                if shared_lock:
                    self.completed.append(current_task)
                    print(f"[>] Reading {current_task.item} on T{current_task.num}")
                    current_transaction.read(current_task)
                    self.transaction_table[current_task.num] = current_task.item
                else:
                    self.incomplete_schedule[current_task.num] = True

            elif(current_task.type == 'W'):
                exclusive_lock = self.exclusive_lock(current_task)
                if exclusive_lock:
                    print(f"[>] Writing {current_task.item} on T{current_task.num}")
                    current_transaction.write(current_task)
                    self.completed.append(current_task)
                else:  
                    self.incomplete_schedule[current_task.num] = True
            print("Queue: ", end="")
            self.printTaskSequence(self.queue)
            print("=========================================")

        print("Final schedule: ", end="")
        self.printTaskSequence(self.completed)
    
  
if __name__ == "__main__":
  twopl = TwoPL('test4.txt')
  twopl.start()