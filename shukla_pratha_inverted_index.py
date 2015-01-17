import MapReduce
import sys

"""
Inverted Index Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    document_id = record[0]
    text = record[1]
    words = text.split()
    for w in words:
        mr.emit_intermediate(w,document_id)

def reducer(key, list_of_values):
    # key: word
    # list_of_values:document_id 

    inverted_index=[]
    for l in list_of_values:
        inverted_index.append(l)

    output=list(set(inverted_index))
    mr.emit((key,output))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
