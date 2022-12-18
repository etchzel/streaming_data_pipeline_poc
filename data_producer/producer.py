import asyncio
import json
import requests
import time
from kafka import KafkaProducer

async def async_getCoinCapRealTimeData(producer, topic, coin, interval=15):
    url = f'https://api.coincap.io/v2/assets/{coin}'
    while True:
        t_0 = time.time()
        res = requests.get(url)

        if res.status_code == 200:
            raw = json.loads(res.content)
            data = raw['data']
            data['timestamp'] = raw['timestamp']
            producer.send(topic=topic, value=data)
            print(f"Producing {data['id']} data at {time.time()}")
        else:
            print(f'Failed API request at {time.time()}')

        await asyncio.sleep(interval - (time.time() - t_0))

producer = KafkaProducer(
    bootstrap_servers='broker:29092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

async def main():
    await asyncio.gather(
        async_getCoinCapRealTimeData(producer, 'topic_BTC', 'bitcoin'),
        async_getCoinCapRealTimeData(producer, 'topic_ETH', 'ethereum')
    )

asyncio.run(main())
