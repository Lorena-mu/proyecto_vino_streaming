import time
import json
import random
from kafka import KafkaProducer

# Configuración del productor Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Función para generar datos simulados
def generate_sample():
    return {
        "fixed_acidity": round(random.uniform(6.0, 10.0), 2),
        "volatile_acidity": round(random.uniform(0.2, 1.2), 2),
        "citric_acid": round(random.uniform(0.0, 1.0), 2),
        "residual_sugar": round(random.uniform(1.0, 10.0), 2),
        "density": round(random.uniform(0.990, 1.005), 4),
        "alcohol": round(random.uniform(8.0, 14.0), 2),
        "quality": random.randint(3, 8)
    }

# Bucle principal para enviar datos al topic
while True:
    sample = generate_sample()
    producer.send('wine_quality', value=sample)
    print("Enviado:", sample)
    time.sleep(1)
