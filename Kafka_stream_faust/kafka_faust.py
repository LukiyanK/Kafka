import faust
app = faust.App('myapp', broker='kafka://localhost:9092')

# Models describe how messages are serialized:
# {"account_id": "3fae-...", amount": 3}
class Order(faust.Record):
    account_id: str
    amount: int

@app.agent(value_type=Order)
async def order(orders):
    async for order in orders:
        # process infinite stream of orders.
        print(f'Order for {order.account_id}: {order.amount}')


# data sent to 'clicks' topic sharded by URL key.
# e.g. key="http://example.com" value="1"
click_topic = app.topic('clicks', key_type=str, value_type=int)

# default value for missing URL will be 0 with `default=int`
counts = app.Table('click_counts', default=int)

@app.agent(click_topic)
async def count_click(clicks):
    async for url, count in clicks.items():
        counts[url] += count
