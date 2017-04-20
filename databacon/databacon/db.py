from datahog.pool import GreenhouseConnPool

pool = None
def connect(shard_config):
  global pool 
  pool = GreenhouseConnPool(shard_config)
  pool.start()
  if not pool.wait_ready(shard_config.get('timeout', 2.)):
    raise Exception("postgres connection timeout")
  return pool

