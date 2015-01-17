import MapReduce
import sys

"""
Asymmetric Friendship in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: friend
    # value: person
    person = record[0]
    friend = record[1]
    mr.emit_intermediate((person,friend), 1)#to check person to friend friendship
    mr.emit_intermediate((friend,person), 1)#to check friend to person friendship
   

def reducer(key, list_of_values):
    # key: friend
    # list_of_values: person
   if len(list_of_values) < 2:#omits symmetric frienships since they have greater count
       mr.emit(key)
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
