import psycopg2
conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='gis_data',
                user='postgres',
                password='password')
print(conn)