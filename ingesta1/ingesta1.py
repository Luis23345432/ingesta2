import mysql.connector
import csv
import boto3

# Configuración de conexión
host = '34.195.53.21'
port = 3308
database = 'scooter_management'
user = 'root'
password = 'utec'

# Tablas y archivos CSV a generar
tablas = {
    'scooters': 'scooters.csv',
    'rides': 'rides.csv'
}

# Conectar a la base de datos y generar archivos CSV
try:
    conexion = mysql.connector.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    if conexion.is_connected():
        print(f'Conectado a la base de datos {database}')
        cursor = conexion.cursor()

        # Iterar sobre las tablas y generar un CSV para cada una sin encabezados
        for tabla, fichero_csv in tablas.items():
            cursor.execute(f'SELECT * FROM {tabla}')
            registros = cursor.fetchall()

            # Guardar los datos en el archivo CSV sin encabezado
            with open(fichero_csv, 'w', newline='') as archivo_csv:
                escritor_csv = csv.writer(archivo_csv)
                escritor_csv.writerows(registros)  # Escribir solo las filas, sin encabezados
            print(f'Registros de {tabla} guardados en {fichero_csv}')

except mysql.connector.Error as error:
    print(f'Error al conectar a MySQL: {error}')
finally:
    if 'conexion' in locals() and conexion.is_connected():
        cursor.close()
        conexion.close()
        print('Conexión MySQL cerrada')

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
