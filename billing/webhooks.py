import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from ipaddress import IPv6Address, IPv6Network, ip_address

from aiohttp import ClientError, ClientSession, ClientTimeout, DummyCookieJar, TCPConnector
from aiohttp.abc import AbstractResolver, ResolveResult
from aiohttp.resolver import ThreadedResolver
from yarl import URL

from billing.domain import PaymentProcessed

GLOBAL_UNICAST = IPv6Network("2000::/3")


class WebhookDeliveryError(Exception):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def normalize_host(host: str) -> str:
    return host.lower().rstrip(".")


def is_public(address: str) -> bool:
    ip = ip_address(address)
    if isinstance(ip, IPv6Address):
        if ip.ipv4_mapped is not None:
            ip = ip.ipv4_mapped
        elif ip not in GLOBAL_UNICAST or ip.sixtofour is not None or ip.teredo is not None:
            return False
    return ip.is_global and not ip.is_multicast and not ip.is_reserved


class PublicResolver(AbstractResolver):
    def __init__(self, private_hosts: frozenset[str]) -> None:
        self.private_hosts = private_hosts
        self.resolver = ThreadedResolver()

    async def resolve(
        self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_UNSPEC
    ) -> list[ResolveResult]:
        addresses = await self.resolver.resolve(host, port, family)
        if normalize_host(host) not in self.private_hosts:
            addresses = [record for record in addresses if is_public(record["host"])]
        if not addresses:
            raise WebhookDeliveryError("destination_blocked")
        return addresses

    async def close(self) -> None:
        await self.resolver.close()


class WebhookClient:
    def __init__(self, session: ClientSession, private_hosts: frozenset[str]) -> None:
        self.session = session
        self.private_hosts = private_hosts

    async def send(self, url: str, event: PaymentProcessed) -> None:
        destination = URL(url)
        host = normalize_host(destination.raw_host or "")
        if destination.scheme not in {"https", "http"} or not host:
            raise WebhookDeliveryError("destination_blocked")
        if host not in self.private_hosts:
            try:
                public = is_public(host)
            except ValueError:
                public = not (":" in host or host.replace(".", "").isdecimal())
            if not public:
                raise WebhookDeliveryError("destination_blocked")
        try:
            async with self.session.post(
                destination,
                json=event.model_dump(mode="json"),
                headers={"Idempotency-Key": str(event.event_id)},
                allow_redirects=False,
            ) as response:
                if not 200 <= response.status < 300:
                    raise WebhookDeliveryError(f"http_{response.status}")
        except TimeoutError:
            raise WebhookDeliveryError("timeout") from None
        except ClientError:
            raise WebhookDeliveryError("network_error") from None


@asynccontextmanager
async def open_webhook_client(
    request_timeout: float, private_hosts: frozenset[str]
) -> AsyncIterator[WebhookClient]:
    hosts = frozenset(map(normalize_host, private_hosts))
    resolver = PublicResolver(hosts)
    try:
        async with ClientSession(
            connector=TCPConnector(resolver=resolver, use_dns_cache=False),
            timeout=ClientTimeout(total=request_timeout),
            cookie_jar=DummyCookieJar(),
            trust_env=False,
        ) as session:
            yield WebhookClient(session, hosts)
    finally:
        await resolver.close()
