from database import redis
import time

streams = {
    'order_completed': 'notification-order-group',
    'refund_order': 'notification-refund-group'
}

for stream, group in streams.items():
    try:
        redis.xgroup_create(stream, group, mkstream=True)
    except:
        print(f'Group {group} already exists!')

while True:
    try:
        for stream, group in streams.items():
            results = redis.xreadgroup(group, 'notification-consumer', {stream: '>'}, count=1, block=1000)
            if results:
                for result in results:
                    obj = result[1][0][1]
                    if stream == 'order_completed':
                        print(f"Obaveštenje: Porudžbina {obj.get('pk', '?')} je uspešno kreirana i plaćena.")
                    elif stream == 'refund_order':
                        print(f"Obaveštenje: Porudžbina {obj.get('pk', '?')} je refundirana.")
    except Exception as e:
        print(f"Notification consumer error: {e}")
    time.sleep(1)
