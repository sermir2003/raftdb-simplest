import asyncio
import aiohttp

async def post_request(session, url, msg):
    try:
        async with session.post(url, json=msg) as response:
            return await response.json()
    except Exception as e:
        return None

async def post_many_requests_with_timeout(work, timeout):
    async with aiohttp.ClientSession() as session:
        tasks = [post_request(session, f'{address}/raft_request', msg) for address, msg in work]
        try:
            results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
            return results
        except asyncio.TimeoutError:
            return "Timeout: Not all requests completed in time."

def simple_broadcast(work, timeout):
    _ = asyncio.run(post_many_requests_with_timeout(work, timeout))
