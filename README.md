# polomapy

Poloma wrapper for psycopg2.extensions.connection class.

## Install

Go to /polomapy in the github and run `python3 setup.py install`. If you are using a specific environment run `source activate <env>` before installing.

## At a Glance:

1. Set environment variable:
 `export POLOMA_PASS=<password>`

2. Import package
```python
from polomapy import PolomaConn
```

3. Make connection and query database.
```python
c = PolomaConn()
res = c.query('select * from test.test')
```
Get columns:

```python
col = c.columns
```

4. Insert to database.
```python
c = PolomaConn()
nested = [ ['this','is'],['a','test'] ]
c.insert('test.test',nested)
```

5. Execute on database.
```python
c = PolomaConn()
c.execute("update test.test set column = 'test'")
```

6. Create Async Buffer. The buffer spawns a number of workers that take from the buffer and send each batch to the database in a separate thread.
```python
from polomapy import PolomaBuff
table = 'test.test'
b = PolomaBuff(	table	
                workers=4, # set number of processes
                maxconn = 8, # set maximum postgres connections
                maxbuff = 50000, # set buffer size to be held in memory
                batchsize = 5000) # set batchsize to send to postgres

for i in range(10):
    values = ('this','is','a','tuple')
    b.append(values) # send rows one at a time

batch = []
for i in range(10):
    batch.append( ('this','is','a','tuple') )

b.append(batch,batch=True) # send batch of rows rather than individual row


b.kill() # kill threads once buffer is empty
```