import time
import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# preson_1 postcodes > 50 
# person_2 postcodes <= 50 
valdas_dbvs = { 'user' : 'valdas', 'password' : 'Valdas123456', 'ip_address' : '20.113.59.203','above_keyspace' : 'person_1', 'below_keyspace' : 'person_2'}
dominykas_dbvs = { 'user' : 'domrap', 'password' : 'Password123*', 'ip_address' : '13.74.59.114','above_keyspace' : 'a50', 'below_keyspace' : 'b50'}

def connet_to_server(username, password, ipAddress):
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

# -----------------------------------------------------------
def get_all_rows_from_table (session, table):
    data = session.execute(f"SELECT * FROM {table}")

    return data

def define_keyspace_d(session_d, departamentName):
    change_keyspace(session_d, dominykas_dbvs['above_keyspace'])
    data = session_d.execute(f"SELECT COUNT(*) FROM library WHERE departament = '{departamentName}'")
    if data[0].count == 0:
        change_keyspace(session_d, dominykas_dbvs['below_keyspace'])


def define_keyspace_v(session_v, personalCode):
    change_keyspace(session_v, valdas_dbvs['above_keyspace'])
    data = session_v.execute(f"SELECT COUNT(*) FROM person WHERE personal_code = {personalCode}")
    if data[0].count == 0:
        change_keyspace(session_v, valdas_dbvs['below_keyspace'])


def insert_takeaway(session_d, session_v, takeaway):
    tableName = 'takeaway'
    define_keyspace_d(session_d, takeaway['department'])
    define_keyspace_v(session_v, takeaway['personalCode'])

    session_d.execute(f"INSERT INTO {tableName} (takeaway_id, copy_id, department, return_date, take_date) VALUES ({takeaway['id']}, {takeaway['copy_id']}, '{takeaway['department']}', '{takeaway['return_date']}', '{takeaway['take_date']}')")
    session_v.execute(f"INSERT INTO {tableName} (takeaway_id, copy_id, department, return_date, take_date, personal_code) VALUES ({takeaway['id']}, {takeaway['copy_id']}, '{takeaway['department']}', '{takeaway['return_date']}', '{takeaway['take_date']}', {takeaway['personalCode']})")

def update_takeaway(session_d, session_v, takeaway):
    tableName = 'takeaway'
    define_keyspace_d(session_d, takeaway['department'])
    define_keyspace_v(session_v, takeaway['personalCode'])

    session_v.execute(f"UPDATE {tableName} SET copy_id={d_row[1]}, department='{d_row[2]}', return_date=NULL, take_date='{d_row[4]}' WHERE takeaway_id={d_row[0]}")

    session_d.execute(f"UPDATE {tableName} SET copy_id = {takeaway['copy_id']}, department = '{takeaway['department']}', return_date = '{takeaway['return_date']}', take_date) VALUES ({takeaway['id']}, , , , '{takeaway['take_date']}')")
    session_v.execute(f"INSERT INTO {tableName} (takeaway_id, copy_id, department, return_date, take_date, personal_code) VALUES ({takeaway['id']}, {takeaway['copy_id']}, '{takeaway['department']}', '{takeaway['return_date']}', '{takeaway['take_date']}', {takeaway['personalCode']})")


def update_libarary_departmentName(session_d, session_v, oldName, newName):
    define_keyspace_d(session_d, oldName)
    data = session_d.execute(f"SELECT * FROM library WHERE departament = '{oldName}'")[0]

    ids = session_d.execute(f"SELECT takeaway_id FROM takeaway WHERE department = '{oldName}' ALLOW FILTERING")
    session_d.execute(f"DELETE FROM library WHERE departament = '{oldName}'")

    session_d.execute(f"INSERT INTO library (departament, address, post_code ) VALUES ('{newName}', '{data[1]}', '{data[2]}')")
    
    for id in ids:

        session_d.execute(f"UPDATE takeaway SET department  = '{newName}' WHERE takeaway_id = {id[0]}")

        change_keyspace(session_v, valdas_dbvs['above_keyspace'])
        data = session_v.execute(f"SELECT COUNT(*) FROM takeaway WHERE takeaway_id = {id[0]}")

        if data[0].count == 0:
            change_keyspace(session_v, valdas_dbvs['below_keyspace'])

        
        session_v.execute(f"UPDATE takeaway SET department = '{newName}' WHERE takeaway_id = {id[0]}")

def delete_book_copy_in_keyspace(session_d, session_v, copy_id, key_space):
    change_keyspace(session_d, dominykas_dbvs[key_space])
    change_keyspace(session_v, valdas_dbvs[key_space])

    ids = session_d.execute(f"SELECT takeaway_id FROM takeaway WHERE copy_id = {copy_id} ALLOW FILTERING")
    session_d.execute(f"DELETE FROM copy WHERE copy_id = {copy_id}")

    for id in ids:
        session_d.execute(f"DELETE FROM takeaway WHERE takeaway_id = {id[0]}")
        session_v.execute(f"DELETE FROM takeaway WHERE takeaway_id = {id[0]}")

def delete_book_copy(session_d, session_v, copy_id):
    delete_book_copy_in_keyspace(session_d, session_v, copy_id, 'above_keyspace')
    delete_book_copy_in_keyspace(session_d, session_v, copy_id, 'below_keyspace')

def get_person_books_in_keyspace(session_d, session_v, perosonal_code, key_space):
    change_keyspace(session_d, dominykas_dbvs[key_space])
    books = []
    define_keyspace_v(session_v, perosonal_code)
    takeaways = session_v.execute(f"SELECT copy_id FROM takeaway WHERE  personal_code = {perosonal_code} ALLOW FILTERING")
    for copy in takeaways:
        book = session_d.execute(f"SELECT isbn FROM copy WHERE copy_id = {copy[0]}")
        books.append(book[0][0])
    return books

def get_person_books(session_d, session_v, perosonal_code):
    books = get_person_books_in_keyspace(session_d, session_v, perosonal_code, 'above_keyspace')
    books = books + (get_person_books_in_keyspace(session_d, session_v, perosonal_code, 'below_keyspace'))
    return books
    
    
if __name__ == "__main__":
    [dominykasCluster, dominykasSession] = connet_to_server(dominykas_dbvs['user'], dominykas_dbvs['password'], dominykas_dbvs['ip_address'])
    change_keyspace(dominykasSession, dominykas_dbvs['above_keyspace'])

    [cluster, session] = connet_to_server(valdas_dbvs['user'], valdas_dbvs['password'], valdas_dbvs['ip_address'])

    #change_keyspace(session, valdas_dbvs['above_keyspace'])
    #sync_takeaways(dominykasSession, session, 'below_keyspace')
    #test_run(session)
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

    # Insert takeaway
    #takeAwayData = { 'id':1222, 'copy_id': 3, 'personalCode': 39909036666, 'department': "Vilnius", 'return_date': "2022-01-12", 'take_date': "2022-01-12"}
    #insert_takeaway(dominykasSession, session, takeAwayData)
    # Update libary name
    #update_libarary_departmentName(dominykasSession, session, 'Klaipeda', 'CCCCCCC')
    # Delete book copy
    #delete_book_copy(dominykasSession, session, 5)
    # Get books by person id
    #print(get_person_books(dominykasSession, session, 39909036666))