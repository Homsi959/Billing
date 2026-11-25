from faststream.rabbit import Channel, QueueType, RabbitBroker, RabbitExchange, RabbitQueue

from billing.payments import DEAD_ROUTE, NEW_ROUTE

payments_exchange = RabbitExchange("payments", durable=True)
dead_exchange = RabbitExchange("payments.dlx", durable=True)
dead_queue = RabbitQueue(DEAD_ROUTE, queue_type=QueueType.QUORUM)
payments_queue = RabbitQueue(
    NEW_ROUTE,
    queue_type=QueueType.QUORUM,
    arguments={
        "x-single-active-consumer": True,
        "x-dead-letter-exchange": dead_exchange.name,
        "x-dead-letter-routing-key": DEAD_ROUTE,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    },
)


def make_broker(url: str) -> RabbitBroker:
    return RabbitBroker(
        url,
        timeout=5,
        graceful_timeout=35,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
    )


async def declare_topology(broker: RabbitBroker) -> None:
    dead = await broker.declare_queue(dead_queue)
    await dead.bind(await broker.declare_exchange(dead_exchange), routing_key=DEAD_ROUTE)
    pending = await broker.declare_queue(payments_queue)
    await pending.bind(await broker.declare_exchange(payments_exchange), routing_key=NEW_ROUTE)
