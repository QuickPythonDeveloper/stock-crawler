import aiohttp
import os
import sys

from .custom_logger import get_logger
from ..config import MIDDLEWARE_ROUTES, MIDDLEWARE_SECRET_KEY, MIDDLEWARE_URL


async def make_log(err: str, url: str = None, company: int = None, data: dict = None) -> None:
    exc_type, exc_obj, exc_tb = sys.exc_info()
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    log = {
        'file_name': fname,
        'line': exc_tb.tb_lineno,
        'error': err,
        'url': url,
        'company': company,
        'data': data
    }
    while 1:
        try:
            status = await hook_to_middleware("create_log", log)
            break
        except Exception as e:
            pass


async def hook_to_middleware(route, data):
    logger = await get_logger()
    route = MIDDLEWARE_ROUTES[route]
    base_url = MIDDLEWARE_URL
    try:
        headers = {"token": MIDDLEWARE_SECRET_KEY}
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    url=f"{MIDDLEWARE_URL}{route}",
                    json=data,
                    headers=headers
            ) as response:
                if response.status == 400:
                    txt = await response.text()
                    print("ok")
                    # logger.error(f"error: {await response.text()}, data: {data}, "
                    #              f"url: {MIDDLEWARE_URL}{MIDDLEWARE_ROUTES[route]}")
                    # await make_log(str(await response.text()), data=data,
                    #                url=f'{MIDDLEWARE_URL}{MIDDLEWARE_ROUTES[route]}')
                elif response.status == 403:
                    logger.error('forbidden')
                elif response.status == 500:
                    resp = await response.text()
                    logger.error(
                        f"Internal server data: {data}, url: {MIDDLEWARE_URL}{MIDDLEWARE_ROUTES[route]}, error: {resp}"
                    )
                    await make_log(
                        f"Internal server data: {data}, url: {MIDDLEWARE_URL}{MIDDLEWARE_ROUTES[route]}, error: {resp}",
                        data=data, url=f'{MIDDLEWARE_URL}{MIDDLEWARE_ROUTES[route]}')
                return response.status
    except Exception as e:
        # pass
        logger.error(f"exit Webhook with error: {e}")


async def get_data_from_middleware(route, data, pk: int = None, page_size: int = None):
    logger = await get_logger()
    try:
        headers = {"token": MIDDLEWARE_SECRET_KEY}
        params = {
            'type': data
        }
        if page_size is not None:
            params.update({'page_size': page_size})
        session_timeout = aiohttp.ClientTimeout(total=None, sock_connect=100, sock_read=100)
        url_route = MIDDLEWARE_ROUTES[route]
        if pk is not None:
            url_route = url_route.format(pk)
        async with aiohttp.ClientSession(timeout=session_timeout) as session:
            async with session.get(
                    url=f"{MIDDLEWARE_URL}{url_route}",
                    params=params,
                    headers=headers
            ) as response:
                return await response.json()
    except Exception as e:
        # pass
        logger.error(f"exit get data from middleware with error: {e}")
