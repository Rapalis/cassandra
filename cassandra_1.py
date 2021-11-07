import time
import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# preson_1 postcodes > 50 
# person_2 postcodes <= 50 
my_keyspaces = ['person_1', 'person_2']
dominykas_keyspaces = ['a50', 'b50']

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

# Atriknimas gauti user'io rekomenduojamu knygu informacija
def get_recommended_books(taxcode: str, session_v, session_d):
    recommendations = session_v.execute(f'SELECT * FROM recommendation WHERE personal_code={taxcode}')

    recommended_books = []
    books = []
    for row in recommendations:
        first = True
        for column in row:
            if first:
                first = False
                continue
            if column is not None:
                recommended_books.append(column)
                book = session_d.execute(f"SELECT * FROM book WHERE isbn='{column}'").one()
                if book is not None:
                    books.append(book)
    print(recommended_books)
    print(books)

# test data for book recommendations
def create_books_for_test(session_d):

    books_create = [
        {
            "isbn": '9786094443404', "age_group": 'Teenagers', "genre": 'Novel', "language": 'English', "name": 'Peony_in_Love', "page_count": 300
        },
        {
            "isbn": '9789955139959', "age_group": 'Adults', "genre": 'Adventure', "language": 'English', "name": 'Ghosts', "page_count": 400
        },
        {
            "isbn": '9786094794339', "age_group": 'Kids', "genre": 'Horror', "language": 'English', "name": 'Haacherot', "page_count": 100
        }, 
        {
            "isbn": '9786094790942', "age_group": 'Teenagers', "genre": 'Fantasy', "language": 'English', "name": 'The_Dry', "page_count": 250
        }
    ]

    for book in books_create:
        session_d.execute(f"INSERT INTO book (isbn, age_group, genre, language, name, page_count) VALUES ('{book['isbn']}', '{book['age_group']}', '{book['genre']}', '{book['language']}', '{book['name']}', {book['page_count']})")
    
# Įterpimas, sukuriama nauja knyga ji gali atsirasti prie rekomendaciju jeigu user'is turi panašia knyga    
def create_books_sync_recomendations(session_v, session_d):
    books_create = [
        {
            "isbn": '9789955139867', "age_group": 'Teenagers', "genre": 'Novel', "language": 'English', "name": 'Paris_Echo_2', "page_count": 300
        },
        {
            "isbn": '9786098233339', "age_group": 'Adults', "genre": 'Horror', "language": 'English', "name": 'The_Mother_in_Law', "page_count": 400
        },
        {
            "isbn": '9786090143506', "age_group": 'Kids', "genre": 'Adventure', "language": 'English', "name": 'LE_GHETTO_INTERIEUR', "page_count": 100
        }, 
        {
            "isbn": '9786094790942', "age_group": 'Teenagers', "genre": 'Horror', "language": 'English', "name": 'The_Dry', "page_count": 250
        }
    ]
    for book in books_create:
        session_d.execute(f"INSERT INTO book (isbn, age_group, genre, language, name, page_count) VALUES ('{book['isbn']}', '{book['age_group']}', '{book['genre']}', '{book['language']}', '{book['name']}', {book['page_count']})")
        similar_books = session_d.execute(f"SELECT * FROM book WHERE genre='{book['genre']}' ALLOW FILTERING")
        for sim_book in similar_books:
            #Add column to recommends if needed
            column = session_v.execute(f"SELECT * from system_schema.columns WHERE table_name='recommendation' AND column_name='{book['name'].lower()}' ALLOW FILTERING").one()
            if column is None:
                session_v.execute(f"ALTER TABLE recommendation ADD {book['name']} bigint")
            
            recommended = session_v.execute(f'SELECT * FROM recommendation WHERE {sim_book.name}={sim_book.isbn} ALLOW FILTERING')
            for recommend in recommended:
                print(f"Adding recommendation to: {recommend.personal_code}")
                session_v.execute(f"UPDATE recommendation SET {book['name']}={book['isbn']} WHERE personal_code={recommend.personal_code}")

# Knygos pašalinimas ir sinchronizacija su rekomendacijomis
def remove_book(isbn, session_v, session_d):
    book = session_d.execute(f"SELECT * FROM book WHERE isbn='{isbn}'").one()

    if book is not None:
        column = session_v.execute(f"SELECT * from system_schema.columns WHERE table_name='recommendation' AND column_name='{book.name.lower()}' ALLOW FILTERING").one()
        if column is not None:
            session_v.execute(f"ALTER TABLE recommendation DROP {book.name.lower()} ")
            test_col_remove(book.name, session_v)

    session_d.execute(f"DELETE FROM book WHERE isbn='{isbn}' IF EXISTS")

    test_remove(isbn, session_d)

def test_remove(isbn, session_d):
    book = session_d.execute(f"SELECT * FROM book WHERE isbn='{isbn}'").one()

    print("removed book")
    print(book)

def test_col_remove(name, session_v):
    column = session_v.execute(f"SELECT * from system_schema.columns WHERE table_name='recommendation' AND column_name='{name.lower()}' ALLOW FILTERING").one()

    print("removed column")
    print(column)

if __name__ == "__main__":
    [cluster, session] = connectToServer('valdas', 'Valdas123456', '20.113.59.203')
    [dominykasCluster, dominykasSession] = connectToServer('domrap', 'Password123*', '13.74.59.114')

    change_keyspace(dominykasSession, dominykas_keyspaces[0])

    change_keyspace(session, my_keyspaces[0])

    # test_run(session)

    # change_keyspace(dominykasSession, dominykas_keyspaces[1])

    # test_run(session)

    # Creating books if they dont exist
    # create_books_for_test(dominykasSession)

    # Getting recommended books for user (Select)
    # get_recommended_books(39909021111, session, dominykasSession)

    # Creating books and syncing them to recommendations (Insert,  Update, Alter Add)
    # create_books_sync_recomendations(session, dominykasSession)

    # Deleting book and syncing with recommendations (Select, Delete, Alter Drop)
    # remove_book(9789955139867, session, dominykasSession)