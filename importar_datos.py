from pymysql import connect
import csv


def connection():
    # Datos de conexion a la base de datos
    connection = connect(host='localhost',
                         user='root',
                         password='',
                         db='db_DatosAbiertosApi',
                         charset='utf8',
                         )
    return connection


def insert_data():
    conn = connection()
    cur = conn.cursor()

    with open('homicidios.csv', 'r') as f:
        # Usa el punto y coma o solo la coma como delimitador.
        reader = csv.reader(f, delimiter=';')
        next(reader)
        for row in reader:
            # Reemplaza "homicidios" con el nombre real de la tabla en tu base de datos.
            table_name = "homicidios"
            placeholders = ', '.join(['%s'] * len(row))
            query = f'INSERT INTO {table_name} VALUES ({placeholders})'
            cur.execute(query, row)

    conn.commit()
    cur.close()
    conn.close()
    print("Importacion de datos exitosa")


insert_data()
