class Task():
  def __init__(self, type: str, num: int, item: str):
    self.type = type
    self.num = num
    self.item = item
  
  def __str__(self):
    if(self.item):
      return self.type + str(self.num) + "(" + self.item + ")"
    return self.type + str(self.num)