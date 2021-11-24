import psycopg2
import psycopg2.extras

hostname = 'localhost'
database = 'test'
username = 'postgres'
pwd = 'password'
port_id = 5432

conn= None
cur= None
try:
    with psycopg2.connect(
                host = hostname,
                dbname = database,
                user = username,
                password = pwd,
                port = port_id ) as conn:
    
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        
            cur.execute('DROP TABLE IF EXISTS producer_data')

            create_script= '''CREATE TABLE IF NOT EXISTS producer_data (

                                name varchar(100) NOT NULL ,
                                country varchar(100) NOT NULL ,
                                created_at varchar(100) NOT NULL 
    
                                )'''
            cur.execute(create_script)
            
            # cur.execute('SELECT * FROM producer_data')
            # for record in cur.fetchall():
            #     print(record['value1'])

            
            conn.commit()

    

except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()