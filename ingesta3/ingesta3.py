import psycopg2
import csv
import boto3

# Parámetros de conexión a PostgreSQL
host = '34.195.53.21'
port = 5432  # Cambia el puerto si es diferente
database = 'gestion_usuarios'
user = 'postgres'
password = 'utec'

# Tablas y sus nombres de archivo CSV
tablas = {
    'users': 'users.csv',
    'subscriptions': 'subscriptions.csv',
    'reviews': 'reviews.csv'
}

# Conectar a la base de datos y generar archivos CSV
try:
    conexion = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    print(f'Conectado a la base de datos {database}')
    cursor = conexion.cursor()

    # Iterar sobre las tablas y generar un CSV para cada una sin encabezados
    for tabla, fichero_csv in tablas.items():
        cursor.execute(f'SELECT * FROM {tabla}')
        registros = cursor.fetchall()

        # Guardar los datos en el archivo CSV sin encabezados
        with open(fichero_csv, 'w', newline='') as archivo_csv:
            escritor_csv = csv.writer(archivo_csv)
            escritor_csv.writerows(registros)  # Escribir solo las filas, sin encabezados
        print(f'Registros de {tabla} guardados en {fichero_csv}')

except psycopg2.Error as error:
    print(f'Error al conectar a PostgreSQL: {error}')
finally:
    # Cerrar la conexión a la base de datos si está abierta
    if 'conexion' in locals() and conexion:
        cursor.close()
        conexion.close()
        print('Conexión PostgreSQL cerrada')

# Subir los archivos CSV a S3 en directorios separados
nombreBucket = "proyecto-parcial-cloud"
s3 = boto3.client('s3')

for tabla, fichero_csv in tablas.items():
    # Crear el nombre del directorio basado en el nombre del archivo (sin la extensión)
    nombre_directorio = fichero_csv.replace('.csv', '')
    ruta_s3 = f'{nombre_directorio}/{fichero_csv}'  # Ruta del archivo en el bucket

    try:
        response = s3.upload_file(fichero_csv, nombreBucket, ruta_s3)
        print(f'Archivo {fichero_csv} subido a {nombre_directorio} en el bucket {nombreBucket}')
    except Exception as e:
        print(f'Error al subir el archivo {fichero_csv} a S3: {e}')
