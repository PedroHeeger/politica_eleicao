from cassandra.cluster import Cluster
cluster = Cluster(['cassandra-app'], port=9042)
session = cluster.connect('')

keyspace = """create keyspace if not exists "politica"
with replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"""

session.execute(keyspace)