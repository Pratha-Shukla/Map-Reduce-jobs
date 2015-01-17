import MapReduce
import sys

"""
Matrix multiplication in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: matrix identifier
    # value: contents of matrix in triples format
    key = record[0]
    i=record[1]
    j=record[2]
    val_ij=record[3]
    if key=="a":
           mr.emit_intermediate(key, [i,j,val_ij])#for matrix a key is "a" and send position, element as value  
    else:
           mr.emit_intermediate(key, [j,i,val_ij])#for matrix b key is "b" and send position, element as value  
   

def reducer(key, list_of_values):
    # key: matrix identifier
    # value: contents of matrix in triples format
    
      a={}  
      b={}  
      #a, b will store the value of matrices
      if key=="a":  
           for v in list_of_values:  
                a[(v[0], v[1])]=v[2]  
           for p in mr.intermediate['b']:  
                b[(p[0], p[1])]=p[2]  
           #fill the blanks with zero 
           for i in range(0,5):  
                for j in range(0,5):  
                     if (i,j) not in a.keys():  
                          a[(i,j)]=0  
                     if (j,i) not in b.keys():  
                          b[(j,i)]=0  
           result=0  
           #matrix multiplication  
           for i in range(0,5):  
                for j in range(0,5):  
                     for k in range(0,5):  
                          result+=a[(i,k)]*b[(j,k)]  
                     mr.emit((i,j,result))  
                     result=0 
# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
