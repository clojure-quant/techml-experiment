# experiments to reproduce bugs involving techml dataset



## and operation wrong (when nil is in column)

evaluate demo.and error 

## random filter

- demo.randomfilter
- evaluate the namespace, and evaluate the last row multiple times;
  it always gets different number of rows. it should return the same
  number of rows
- if the row filter does NOT contain this expressions, then the 
  filtering is working: (:protective-pivot-low row)                           (:protective-pivot-high row)
  
