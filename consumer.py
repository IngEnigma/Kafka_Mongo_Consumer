from flask import Flask
from confluent_kafka import Consumer, KafkaException
import json
import threading
from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId

app = Flask(__name__)

# Configuración de Redpanda
KAFKA_CONFIG = {
    'bootstrap.servers': 'cvq4abs3mareak309q80.any.us-west-2.mpx.prd.cloud.redpanda.com:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-256',
    'sasl.username': 'IngEnigma',
    'sasl.password': 'BrARBOxX98VI4f2LIuIT1911NYGrXu',
    'group.id': 'crimes-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': 'false'
}

# Configuración de MongoDB
MONGO_URI = "mongodb+srv://IngEnigma:0ZArHx18XQIFWPHu@bigdata.iwghsuv.mongodb.net/?retryWrites=true&w=majority&appName=BigData"
DB_NAME = "BigData"
COLLECTION_NAME = "BigData"  

TOPIC = "crimes_mongo"  # Debe coincidir con el tópico del producer

def get_mongo_client():
    return MongoClient(MONGO_URI)

def insert_crime(data):
    try:
        client = get_mongo_client()
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # Convertir string dates a objetos datetime usando el formato que contempla microsegundos
        date_format = '%Y-%m-%d %H:%M:%S.%f'
        
        if 'crime_details' in data and 'report_date' in data['crime_details']:
            if isinstance(data['crime_details']['report_date'], str):
                data['crime_details']['report_date'] = datetime.strptime(
                    data['crime_details']['report_date'], date_format)
        
        if 'metadata' in data and 'imported_at' in data['metadata']:
            if isinstance(data['metadata']['imported_at'], str):
                data['metadata']['imported_at'] = datetime.strptime(
                    data['metadata']['imported_at'], date_format)
        
        # Insertar o actualizar (upsert)
        result = collection.update_one(
            {'_id': data['_id']},
            {'$set': data},
            upsert=True
        )
        
        client.close()
        
        if result.upserted_id:
            print(f"Documento insertado: ID {data['_id']}")
            return True
        elif result.modified_count > 0:
            print(f"Documento actualizado: ID {data['_id']}")
            return True
        else:
            print(f"Documento no modificado: ID {data['_id']}")
            return False
            
    except Exception as e:
        print(f"Error al insertar en MongoDB: {e}")
        return False

def kafka_consumer_loop():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])
    print(f"Consumer iniciado. Suscrito al tópico: {TOPIC}")

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                print("No hay nuevos mensajes...")
                continue
                
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    print("Fin de partición alcanzado")
                    continue
                else:
                    print(f"Error al consumir: {msg.error()}")
                    break

            try:
                print(f"\nMensaje recibido. Offset: {msg.offset()}")
                crime_data = json.loads(msg.value().decode('utf-8'))
                print("Datos parseados:", json.dumps(crime_data, indent=2))
                
                if insert_crime(crime_data):
                    # Confirmar offset solo si se procesó correctamente
                    consumer.commit(asynchronous=False)
                
            except json.JSONDecodeError as e:
                print(f"Error decodificando JSON: {e}")
                print("Mensaje problemático:", msg.value())
            except Exception as e:
                print(f"Error procesando mensaje: {e}")

    except KeyboardInterrupt:
        print("\nDeteniendo consumer...")
    except Exception as e:
        print(f"Error inesperado en el consumer: {e}")
    finally:
        consumer.close()
        print("Consumer cerrado")

@app.route("/health")
def health():
    return "ok", 200

if __name__ == '__main__':
    # Iniciar thread del consumer
    consumer_thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    consumer_thread.start()
    
    # Iniciar aplicación Flask
    app.run(host="0.0.0.0", port=8080)
