from logging import NullHandler
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# preson_1 postcodes > 50 
# person_2 postcodes <= 50 
my_keyspaces = ['person_1', 'person_2']
dominykas_keyspaces = ['a50', 'b50']

cluster = None
session = None

def connectToServer(username, password, ipAddress):
    auth_provider = PlainTextAuthProvider(username = username, password = password)
    cluster = Cluster([ipAddress], auth_provider=auth_provider)
    
    session = cluster.connect()
    return [cluster, session]

def test_run(session):
    users = session.execute('SELECT * FROM person')

    for row in users:
        print(row)


def change_keyspace(session, keyspace):
    session.execute(f'USE {keyspace}')


if __name__ == "__main__":
    #cluster, session] = connectToServer('valdas', 'Valdas123456', '20.113.59.203')
    [dominykasCluster, dominykasSession] = connectToServer('domrap', 'Password123*', '13.74.59.114')

    change_keyspace(dominykasSession, dominykas_keyspaces[0])

    test_run(dominykasSession)

    change_keyspace(dominykasSession, dominykas_keyspaces[1])

    test_run(dominykasSession)