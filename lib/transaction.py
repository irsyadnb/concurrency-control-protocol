from lib.task import Task

class Transaction:
  def __init__(self, num: int):
    self.num = num
    self.read_set = set()
    self.write_set = set()
    self.is_commit = False
  
  def read(self, task: Task):
    self.read_set.add(task.item)
    
  def write(self, task: Task):
    self.write_set.add(task.item)
  
  def commit(self):
    self.is_commit = True
  
  def __str__(self) -> str:
    return "T" + str(self.num) + \
      ", read_set: " + str(self.read_set) + ", write_set: " + str(self.write_set) +', commit: '+ str(self.is_commit)