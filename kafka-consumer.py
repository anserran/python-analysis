import sys
from kafka import KafkaConsumer
from redlock import Redlock

if len(sys.argv) < 4:
    print('Invoke the script in the form "python kafka-consumer.py kafka_connection_string topic traces_path"')
    exit()

kafka_connection = sys.argv[1]
topic = sys.argv[2]
traces_path = sys.argv[3]

if not traces_path[-1] == '/':
    traces_path += "/"

# To consume messages
consumer = KafkaConsumer(topic,
                         group_id='traces_cache',
                         bootstrap_servers=[kafka_connection])
consumer.commit()
dlm = Redlock([{"host": "localhost", "port": 6379, "db": 0}, ])
for message in consumer:
    key = message.key.decode('utf-8')
    lock = False
    while not lock:
        lock = dlm.lock(key, 5000)

    with open(traces_path + key, 'ab') as f:
        f.write(message.value)

    dlm.unlock(lock)

    consumer.task_done(message)
    consumer.commit()
