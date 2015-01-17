import MapReduce
import sys

"""
Unique trims(DNA) Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: DNA identifier
    # value: DNA pattern
   key = record[0]  
   sequence=record[1]  
   dna_trim=sequence[:-10]  
   mr.emit_intermediate(dna_trim, 0)

def reducer(key, list_of_values):
    # key: Unique trimmed pattern
    
    mr.emit(key)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
