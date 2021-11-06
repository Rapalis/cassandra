from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# preson_1 postcodes > 50 
# person_2 postcodes <= 50 
my_keyspaces = ['person_1', 'person_2']


def connect_valdas():
    auth_provider = PlainTextAuthProvider(username='valdas', password='Valdas123456')
    cluster = Cluster(['20.113.59.203'], auth_provider=auth_provider)
    session = cluster.connect()

    return cluster, session

def test_run(session):
    session.execute('USE {}'.format(my_keyspaces[0]))

    users = session.execute('SELECT * FROM recommendation')

    for row in users:
        print(row)


if __name__ == "__main__":
    cluster, session = connect_valdas()

    test_run(session)