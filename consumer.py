from flask import Flask
from confluent_kafka import Consumer, KafkaException
import json
import threading
from pymongo import MongoClient
from datetime import datetime

app = Flask(__name__)

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

MONGO_URI = "mongodb+srv://IngEnigma:0ZArHx18XQIFWPHu@bigdata.iwghsuv.mongodb.net/?retryWrites=true&w=majority&appName=BigData"
DB_NAME = "BigData"
COLLECTION_NAME = "BigData"
TOPIC = "crimes_mongo"
DATE_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME][COLLECTION_NAME], client

def parse_dates(data: dict):
    try:
        if 'crime_details' in data and 'report_date' in data['crime_details']:
            date_str = data['crime_details']['report_date']
            if isinstance(date_str, str):
                data['crime_details']['report_date'] = datetime.strptime(date_str, DATE_FORMAT)
        
        if 'metadata' in data and 'imported_at' in data['metadata']:
            date_str = data['metadata']['imported_at']
            if isinstance(date_str, str):
                data['metadata']['imported_at'] = datetime.strptime(date_str, DATE_FORMAT)
    except Exception as e:
        print(f"Error al parsear fechas: {e}")

def insert_crime(data):
    try:
        collection, client = get_mongo_collection()
        parse_dates(data)

        result = collection.update_one(
            {'_id': data['_id']},
            {'$set': data},
            upsert=True
        )
        client.close()

        if result.upserted_id:
            print(f"Documento insertado: ID {data['_id']}")
        elif result.modified_count > 0:
            print(f"Documento actualizado: ID {data['_id']}")
        else:
            print(f"Documento no modificado: ID {data['_id']}")
        return True
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
                continue

            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    continue
                print(f"Error al consumir: {msg.error()}")
                break

            try:
                print(f"\nMensaje recibido. Offset: {msg.offset()}")
                data = json.loads(msg.value().decode('utf-8'))
                print("Datos parseados:", json.dumps(data, indent=2))

                if insert_crime(data):
                    consumer.commit(asynchronous=False)

            except json.JSONDecodeError as e:
                print(f"Error JSON: {e} - Mensaje: {msg.value()}")
            except Exception as e:
                print(f"Error procesando mensaje: {e}")

    except KeyboardInterrupt:
        print("\nConsumer detenido por teclado.")
    except Exception as e:
        print(f"Error inesperado: {e}")
    finally:
        consumer.close()
        print("Consumer cerrado.")
        
@app.route("/health")
def health_check():
    return "ok", 200

def main():
    consumer_thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    consumer_thread.start()
    app.run(host="0.0.0.0", port=8080)

if __name__ == '__main__':
    main()
