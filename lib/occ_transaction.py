from .task import Task

class OCCTransaction:
  def __init__(self, num: int):
    self.num = num
    self.read_set = []
    self.write_set = []
    self.start = 0
    self.finish = 0
    self.validation = 0
    self.is_commit = False
  
  def read(self, task: Task):
    self.read_set.append(task.item)
    
  def write(self, task: Task):
    self.write_set.append(task.item)
  
  def commit(self):
    self.is_commit = True
  
  def __str__(self) -> str:
    return "T" + str(self.num) + \
      ", read_set: " + str(self.read_set) + ", write_set: " + str(self.write_set) + \
        ", start_time: " + str(self.start) + ", finish_time: " + str(self.finish) + \
          " is_commit: " + str(self.is_commit)