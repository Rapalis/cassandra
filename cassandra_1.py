from logging import NullHandler
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# preson_1 postcodes > 50 
# person_2 postcodes <= 50 
my_keyspaces = ['person_1', 'person_2']

cluster = None
session = None

def connect_valdas():
    global cluster
    global session

    auth_provider = PlainTextAuthProvider(username='valdas', password='Valdas123456')
    cluster = Cluster(['20.113.59.203'], auth_provider=auth_provider)
    session = cluster.connect()

def test_run():
    global session

    users = session.execute('SELECT * FROM person')

    for row in users:
        print(row)


def change_keyspace(index: int):
    global session

    session.execute('USE {}'.format(my_keyspaces[index]))


if __name__ == "__main__":
    connect_valdas()

    change_keyspace(0)

    test_run()

    change_keyspace(1)

    test_run()