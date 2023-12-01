from lib.task import Task
from lib.occ_transaction import OCCTransaction
from lib.transaction import Transaction

def parse(filename: str, parser_type: str):
  f = open('test/' + filename, 'r')
  tasks = [line.strip() for line in f.readlines()]

  num_transactions = 0
  task_list = []
  transactions = []

  for t in tasks:
    if t[0].upper() == 'W' or t[0].upper() == 'R' and '(' in t and ')' in t:
      open_bracket_idx = t.index('(')
      close_bracket_idx = t.index(')')
      
      if open_bracket_idx > close_bracket_idx:
        print("Invalid Bracket Input")
        break
      
      if(int(t[1:open_bracket_idx]) > num_transactions):
        num_transactions = int(t[1:open_bracket_idx])
        
      task_list.append(Task(t[0], int(t[1:open_bracket_idx]), t[open_bracket_idx+1:close_bracket_idx]))
      
    if t[0].upper() == 'C':
      task_list.append(Task(t[0], int(t[1:open_bracket_idx]), ''))
  
  if parser_type.lower() == 'occ':
    transactions = [OCCTransaction(i) for i in range(1, num_transactions + 1)]
    return num_transactions, task_list, transactions
  else:
    transactions = [Transaction(i) for i in range(1, num_transactions + 1)]
    return num_transactions, task_list, transactions


def write(fileName: str, data: list): 
    file = open("out/" + fileName, "w")

    for d in data:
        file.write(f'{d}\n')
    
    file.close()