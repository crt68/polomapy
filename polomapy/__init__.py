
from psycopg2 import extensions, extras, pool
import sys, time, os
from psutil import cpu_percent, virtual_memory
from threading import Thread, Lock
from multiprocessing import cpu_count, Queue, Process, Value, Condition

POLOMA_PASS=os.environ.get('POLOMA_PASS')

if POLOMA_PASS is None:
	print('Sup Asshole. Set POLOMA_PASS env variable.')

def is_nested(nested):
	if any(not isinstance(i, (list,tuple) ) for i in nested):
			raise ValueError('Hey dumbass - you can only dump nested lists/tuples.')

class PolomaConn(extensions.connection):
	def __init__(	self,
					host = 'localhost',
					user = 'postgres',
					port= '5432',
					dbname = 'postgres'):

		dsn = 		'''	dbname='{dbname}'
						user='{user}'
						host='{host}'
						password='{password}'
					'''.format(	dbname=dbname,
								user=user,
								host=host,
								password=POLOMA_PASS )

		super(PolomaConn, self).__init__(dsn=dsn)
		self.cur = self.cursor()

	def _set_columns(self):
		self.columns = [desc.name for desc in self.cur.description]

	def execute(self,query):
		self.cur.execute(query)

	def iter_rows(self,query):
		self.cur.execute(query)
		self._set_columns()
		return self.cur

	def query(self, query):
		self.cur.execute(query)
		fetched = self.cur.fetchall()
		self._set_columns()
		return fetched

	def insert(self, table, nested):
		is_nested(nested)
		template = 'INSERT INTO '+table+' VALUES %s'
		extras.execute_values(self.cur,template,nested)


class PolomaPool:
	def __init__(self, *args, **kwargs): # args/kwargs passed to conn
		self._lock = Lock()
		self._args = args
		self._kwargs = kwargs
		self._pool = []
		self._pool_size = 0
		self._kill = False
		self._master = PolomaConn(*self._args, **self._kwargs)
		self._set_max_clients()
		self._set_curr_clients()

	def _set_max_clients(self):
		self._master.execute('SHOW max_connections;')
		self._max_clients = int( self._master.cur.fetchone()[0] )
				
	def _set_curr_clients(self):
		self._master.execute('SELECT COUNT(*) FROM pg_stat_activity;')
		self.curr_clients = int( self._master.cur.fetchone()[0] )

	def grabconn(self):
		self._lock.acquire() # lock
		
		if self._pool: # take first conn from pool
			conn = self._pool.pop()
			self._lock.release()
			return conn

		self._set_curr_clients()
		if self._max_clients - 15 > self.curr_clients: # leave 10 open
			self._pool_size += 1
			self._lock.release()
			return PolomaConn(*self._args, **self._kwargs)

		else:
			while not self._pool:
				pass # if there are too many conns then wait
			conn = self._pool.pop()
			self._lock.release()
			return conn
		
	def putconn(self, conn):
		"""Put away a connection."""
		self._lock.acquire()
		self._pool.append(conn)
		if self._kill:
			self._try_closing()
		self._lock.release()

	def _pool_full(self):
		return len(self._pool) == self._pool_size

	def _try_closing(self):
		"""Close all connections if everyone is back in pool."""
		if self._pool_full():
			for conn in self._pool:
				conn.close()
			self._master.close()

	def kill(self):
		self._try_closing()
		self._kill = True
		

class PolomaBuff:
	def __init__(	self,
					table,
					workers=cpu_count(),
					maxconn = cpu_count(),
					maxbuff = 50000,
					batchsize = 5000,
					*args, **kwargs):

		self.table = table
		self.maxbuff = maxbuff
		self.maxconn = maxconn
		self.batchsize = batchsize
		self._args = args
		self._kwargs = kwargs
		self._queue = Queue()
		self._buffer_notifier = Condition()
		self._conn_notifier = Condition()
		self._conns = Value('i', 0)
		self._buffsize = Value('i', 0)
		self._sent = Value('i', 0)
		self._workers = 0
		self._buffer = []
		self._procs = []
		self._spawn(workers)
		self._progress()

	def _progress(self):
		print(  '\tSENT:',self._sent.value,
				'BUFFER:',self._buffsize.value,
				'CONNS:',self._conns.value,
				'WORKERS:',self._workers,
				'CPU:',cpu_percent(),
				'MEM:',virtual_memory().percent,
				' '*10, end='\r')

	def _spawn(self,workers):
		for _ in range(workers):
			values = (	self._sent,
						self._buffsize,
						self._conns,
						self._queue,
						self._buffer_notifier,
						self._conn_notifier)
			p = Process(target=self._worker, args=values)
			p.daemon = True
			self._procs.append(p)
			p.start()
			self._workers+=1

	def _worker(self,_sent,
				_buffsize,_conns,
				_queue,_buffer_notifier,
				_conn_notifier):

		def _wait_if_max_conns():
			_conn_notifier.acquire()
			while _conns.value >= self.maxconn:
				_conn_notifier.wait()
			_conn_notifier.release()

		def _send(_conn_notifier,_conns,_sent,_buffer):
			_conns.value+=1
			c = PolomaConn()
			is_nested(_buffer)
			c.insert(self.table,_buffer)
			c.commit()
			c.close()
			_conns.value-=1
			_notify(_conn_notifier)
			_sent.value += len(_buffer)

		def _notify(notifier):
			notifier.acquire()
			notifier.notify()
			notifier.release()

		while True:
			_buffer = _queue.get()
			_buffsize.value -= len(_buffer)
			_notify(_buffer_notifier)
			if _buffer == 'KILL':
				break
			_wait_if_max_conns()		
			Thread( target=_send,args=(_conn_notifier, _conns,_sent,_buffer) ).start()

	def _wait_if_buff_full(self):
		self._buffer_notifier.acquire()
		while self._buffsize.value >= self.maxbuff:
			self._buffer_notifier.wait()
		self._buffer_notifier.release()

	def append(self,item,batch=False):
		if batch:
			self._buffer += item
		else:
			self._buffer.append(item)
	
		self._wait_if_buff_full()

		if len(self._buffer) >= self.batchsize:
			self._buffsize.value += len(self._buffer)
			self._queue.put(self._buffer)
			self._progress()
			self._buffer = []

	def kill(self):
		for _ in range(self._workers):
			self._queue.put('KILL')
		for p in self._procs:
			p.join()
			print()
		
