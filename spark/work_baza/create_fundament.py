
import re
from cassandra.cluster import Cluster

def create_sql(session, sql_file):
    statement = ""
    for line in open(sql_file):
        if re.match(r'--', line):  # ignore sql comment lines
            continue
        if not re.search(r';$', line):  # keep appending lines that don't end in ';'
            statement = statement + line
        else:
            statement = statement + line
            session.execute(statement)
            statement = ""

def create_env_baza():
    cluster = Cluster(['cassandra'])
    session = cluster.connect()
    create_sql(session, 'key.sql')
    #with open(r'key.sql') as f:
    #    sql = f.read()
    #    session.execute(sql)

    session.set_keyspace('test')
    create_sql(session, 'table.sql')

    #with open(r'table.sql') as f:
    #    sql = f.read()
    #    print(sql)
    #    session.execute(sql)
    print("Deployment of the base is complete")
