from lib.occ_transaction import OCCTransaction
from lib.task import Task
from lib.parser import *

class OCC:
  def __init__(self, filename: str):
    num_transactions, task_list, transactions = parse(filename=filename, parser_type='occ')
    self.init_task_list: list[Task] = task_list
    self.task_list: list[Task] = self.init_task_list
    self.num_transactions = num_transactions
    self.transactions = transactions
    self.timestamp = 1
    self.result: list[Task] = []
  
  def validate(self, check_transaction: OCCTransaction):
    valid = True
    self.timestamp += 1
    check_transaction.validation = self.timestamp
    
    tj_startTS = check_transaction.start
    tj_validationTS = check_transaction.validation
    
    for transaction in self.transactions:
      if transaction.num != check_transaction.num:
        ti_finishTS = transaction.finish
        ti_validationTS = transaction.validation

        if(ti_validationTS < tj_validationTS):
          '''
          For all TS(Ti) < < TS(Tj), atleast hold one of these:
          - finishTS(Ti) < startTS(Tj)
          - startTS(Tj) < finishTS(Ti) < validationTS(tj) and the set of data items written
            by Ti does not intersect with the set of data items ready by Tj
          then, validation succeeds, and Tj can be committed
          '''
          if(ti_finishTS < tj_startTS):
            continue
          elif (tj_startTS < ti_finishTS and ti_finishTS < tj_validationTS):
            elmt_intersect = False
            conflict = None
            for write in transaction.write_set:
              if write in check_transaction.read_set:
                elmt_intersect = True
                conflict = write
                break
            if elmt_intersect:
              print("[Found Intersect Element]")
              print(f"T{check_transaction.num} conflict with T{transaction.num} by having an intersect element: {conflict}")
              print("Aborting...")
              valid = False
              break
          else:
            print("Does not met all conditions")
            valid = False
            break
          
    return valid
      
  
  def start(self):
    index = 0
    while index < len(self.task_list):
      current_task = self.task_list[index]
      current_transaction = list(filter(lambda obj: obj.num == current_task.num, self.transactions))[0]
      
      if current_transaction.start == 0:
        current_transaction.start = self.timestamp
      
      if(current_task.type == 'C'):
        print(f"Trying to commit T{current_transaction.num}")
        if self.validate(current_transaction):
          # print("Valid")
          current_transaction.finish = self.timestamp
          current_transaction.commit()
          print(f"T{current_transaction.num} is committed")
          for t in current_transaction.write_set:
            print(f"T{current_transaction.num} writes {t} from temporary local variable to database")
          self.result.append(current_task)
        else:
          print(f"Rolling back T{current_task.num}")
          # Set new StartTS
          current_transaction.start = self.timestamp
          current_transaction.read_set.clear()
          current_transaction.write_set.clear()
                  
          # Make a temp containing tasks before abort
          temp = self.task_list[:index+1].copy()
          
          # Add A (Abort) task
          temp.append(Task("A", current_transaction.num, ""))
          
          # Add conflict transaction tasks to temp
          for task in current_transaction.log_records:
            temp.append(task)
            
          # Add commit task on the conflict transaction
          temp.append(Task("C", current_transaction.num, ""))
          
          not_proccessed = self.task_list[index+1:].copy()
          
          # If there are still not proccessed task, put after rollback
          for task in not_proccessed:
            temp.append(task)
            
          self.task_list = temp 
      
      elif(current_task.type == 'R'):
        print(f"T{current_transaction.num} reads {current_task.item}")
        current_transaction.read(current_task)
        self.result.append(current_task)
        
      elif(current_task.type == 'W'):
        print(f"T{current_transaction.num} writes {current_task.item} to temporary local variable")
        current_transaction.write(current_task)
        self.result.append(current_task)
        
      elif(current_task.type == 'A'):
        self.result.append(current_task)
        
      self.timestamp+=1
      index+=1
    # for t in self.transactions:
    #   print(t)
    print()
    
    print("Initialize Schedule: ")
    for task in self.init_task_list:
      if task.type == 'C':
        print(f"{task.type}{task.num}", end=" ")
      else:
        print(f"{task.type}{task.num}({task.item})", end=" ")
        
    print()
    
    print("Final Schedule: ")
    for task in self.result:
      if task.type == 'C' or task.type == 'A':
        print(f"{task.type}{task.num}", end=" ")
      else:
        print(f"{task.type}{task.num}({task.item})", end=" ")
  
  def writeToFile(self, outputPath: str):
    write(f"result_{outputPath}.txt", self.result)

if __name__ == "__main__":
  occ = OCC('test2.txt')
  occ.start()
  occ.writeToFile('test2')