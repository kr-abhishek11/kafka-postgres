from flask import Flask
from flask.json import jsonify
import psycopg2
import psycopg2.extras
import json

app=Flask(__name__)

conn= psycopg2.connect(database ='test',user='postgres', host='localhost',password= 'password', port= 5432)
cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

cursor.execute("SELECT * from producer_data")
message= cursor.fetchall()

@app.route('/')
def home():
    return 'API used to fetch consumer data from Postgresql'
    
@app.route('/data')
def get_data():
    try:
        #cursor.execute("SELECT * from producer_data")
        
        return jsonify({"Message":message})
        
          
        #  return i
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()
        print("Total number of rows ",cursor.rowcount) 
    
if __name__ =="__main__":
    app.run(debug=True)