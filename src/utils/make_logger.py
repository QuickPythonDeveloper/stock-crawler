import os
import sys

from . import hook_to_middleware


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
