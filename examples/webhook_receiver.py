import json

from aiohttp import web


async def receive(request: web.Request) -> web.Response:
    event = await request.json()
    print(json.dumps(event, ensure_ascii=False), flush=True)
    return web.Response(status=204)


app = web.Application()
app.router.add_post("/webhook", receive)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=8080)
