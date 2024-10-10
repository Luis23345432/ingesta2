import csv
import boto3
from pymongo import MongoClient

# Parámetros de conexión a MongoDB con autenticación
mongo_host = '34.195.53.21'  # Cambia esto por tu host
mongo_port = 27017  # Puerto por defecto de MongoDB
mongo_user = 'admin'  # Usuario raíz de MongoDB
mongo_password = 'password'  # Contraseña raíz de MongoDB
mongo_db = 'gestion_boletas'  # Cambia por tu base de datos
mongo_collection_boleta = 'boletas'
mongo_collection_promotion = 'promotions'
mongo_collection_transaction = 'transactions'

# Parámetros para S3
nombre_bucket = "proyecto-parcial-cloud"
fichero_boletas = 'boletas.csv'
fichero_promotions = 'promotions.csv'
fichero_transactions = 'transactions.csv'

# Construir la URI de conexión con autenticación
mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_db}"

# Conectar a MongoDB
try:
    client = MongoClient(mongo_uri)
    db = client[mongo_db]

    # Colección Boletas
    collection_boleta = db[mongo_collection_boleta]
    print(f'Conectado a MongoDB en la base de datos {mongo_db}, colección {mongo_collection_boleta}')

    # Extraer datos de la colección Boletas
    boletas = list(collection_boleta.find())

    # Generar archivo CSV para Boletas (sin encabezados)
    with open(fichero_boletas, 'w', newline='') as archivo_boletas:
        escritor_boletas = csv.writer(archivo_boletas)
        for boleta in boletas:
            escritor_boletas.writerow(
                [boleta['_id'], boleta['fecha_emision'], boleta['payment_method'], boleta['amount']])

    print(f'Registros de boletas guardados en {fichero_boletas}')

    # Colección Promotions
    collection_promotion = db[mongo_collection_promotion]
    print(f'Conectado a MongoDB en la base de datos {mongo_db}, colección {mongo_collection_promotion}')

    # Extraer datos de la colección Promotions
    promotions = list(collection_promotion.find())

    # Generar archivo CSV para Promotions (sin encabezados)
    with open(fichero_promotions, 'w', newline='') as archivo_promotions:
        escritor_promotions = csv.writer(archivo_promotions)
        for promotion in promotions:
            escritor_promotions.writerow(
                [promotion['_id'], promotion['codigo'], promotion['porcentaje'], promotion['start_date'],
                 promotion['end_date']])

    print(f'Registros de promociones guardados en {fichero_promotions}')

    # Colección Transactions
    collection_transaction = db[mongo_collection_transaction]
    print(f'Conectado a MongoDB en la base de datos {mongo_db}, colección {mongo_collection_transaction}')

    # Extraer datos de la colección Transactions
    transactions = list(collection_transaction.find())

    # Generar archivo CSV para Transactions (sin encabezados)
    with open(fichero_transactions, 'w', newline='') as archivo_transactions:
        escritor_transactions = csv.writer(archivo_transactions)
        for transaction in transactions:
            escritor_transactions.writerow(
                [transaction['_id'], transaction['promotion_id'], transaction['ride_id'], transaction['user_id'],
                 transaction['ride_cost'], transaction['amount']])

    print(f'Registros de transacciones guardados en {fichero_transactions}')

except Exception as e:
    print(f'Error al conectar a MongoDB o al generar los CSV: {e}')

finally:
    if 'client' in locals() and client:
        client.close()
        print('Conexión a MongoDB cerrada')

# Subir archivos a S3 en directorios separados
s3 = boto3.client('s3')

# Subir CSV de Boletas
if fichero_boletas:
    try:
        ruta_s3_boletas = f'boletas/{fichero_boletas}'
        s3.upload_file(fichero_boletas, nombre_bucket, ruta_s3_boletas)
        print(f'Archivo {fichero_boletas} subido al bucket en la ruta {ruta_s3_boletas}')
    except Exception as e:
        print(f'Error al subir {fichero_boletas} a S3: {e}')

# Subir CSV de Promotions
if fichero_promotions:
    try:
        ruta_s3_promotions = f'promotions/{fichero_promotions}'
        s3.upload_file(fichero_promotions, nombre_bucket, ruta_s3_promotions)
        print(f'Archivo {fichero_promotions} subido al bucket en la ruta {ruta_s3_promotions}')
    except Exception as e:
        print(f'Error al subir {fichero_promotions} a S3: {e}')

# Subir CSV de Transactions
if fichero_transactions:
    try:
        ruta_s3_transactions = f'transactions/{fichero_transactions}'
        s3.upload_file(fichero_transactions, nombre_bucket, ruta_s3_transactions)
        print(f'Archivo {fichero_transactions} subido al bucket en la ruta {ruta_s3_transactions}')
    except Exception as e:
        print(f'Error al subir {fichero_transactions} a S3: {e}')
