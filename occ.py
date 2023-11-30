from lib.occ_transaction import OCCTransaction
from lib.task import Task

class OCC:
  def __init__(self):
    self.task_list: list[Task] = []
    self.num_transactions = 0
    self.transactions = []
    self.timestamp = 1
  
  def parse(self, filename):
    f = open(filename, 'r')
    tasks = [line.strip() for line in f.readlines()]
    
    for t in tasks:
      if t[0].upper() == 'W' or t[0].upper() == 'R' and '(' in t and ')' in t:
        open_bracket_idx = t.index('(')
        close_bracket_idx = t.index(')')
        
        if open_bracket_idx > close_bracket_idx:
          print("Invalid Bracket Input")
          break
        
        if(int(t[1:open_bracket_idx]) > self.num_transactions):
          self.num_transactions = int(t[1:open_bracket_idx])
          
        self.task_list.append(Task(t[0], int(t[1:open_bracket_idx]), t[open_bracket_idx+1:close_bracket_idx]))
        
      if t[0].upper() == 'C':
        self.task_list.append(Task(t[0], int(t[1:open_bracket_idx]), ''))
      
    self.transactions = [OCCTransaction(i) for i in range(1, self.num_transactions + 1)]
  
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
        
        # print("---")
        # print(ti_validationTS)
        # print(tj_validationTS)
        # print(ti_finishTS)
        # print(tj_startTS)
        # print(tj_startTS)
        # print(ti_finishTS)
        # print(tj_validationTS)

        if(ti_validationTS < tj_validationTS):
          '''
          For all TS(Ti) < < TS(Tj), atleast hold one of these:
          - finishTS(Ti) < startTS(Tj)
          - startTS(Tj) < finishTS(Ti) < validationTS(tj) and the set of data items written
            by Ti does not intersect with the set of data items ready by Tj
          then, validation succeeds, and Tj can be committed
          '''
          if((ti_finishTS < tj_startTS) or (tj_startTS < ti_finishTS and ti_finishTS < tj_validationTS)):
            elmt_intersect = False
            for write in transaction.write_set:
              if write in check_transaction.read_set:
                elmt_intersect = True
            if elmt_intersect:
              print("Found intersect element")
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
        print("Committing...")
        if self.validate(current_transaction):
          print("Valid")
          current_transaction.finish = self.timestamp
          current_transaction.commit()
        else:
          print("Something is not right")
      
      elif(current_task.type == 'R'):
        print("Reading...")
        current_transaction.read(current_task)
        
      elif(current_task.type == 'W'):
        print("Writing...")
        current_transaction.write(current_task)
        
      self.timestamp+=1
      index+=1
    for t in self.transactions:
      print(t)
      
  
if __name__ == "__main__":
  occ = OCC()
  occ.parse('tc2.txt')
  occ.start()