from datahog.pool import GeventConnPool

pool = None
def connect(shard_config):
  global pool 
  print(shard_config)
  pool = GeventConnPool(shard_config)
  pool.start()
  if not pool.wait_ready(shard_config.get('timeout', 2.)):
    raise Exception("postgres connection timeout")
  return pool

