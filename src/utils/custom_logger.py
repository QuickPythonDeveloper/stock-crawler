import logging
from colorlog import ColoredFormatter


async def get_logger():
    formatter = ColoredFormatter(
        "%(log_color)s%(levelname)s %(asctime)s %(blue)s%(message)s %(purple)s(%(filename)s:%(lineno)d)",
        datefmt="%m/%d/%Y %I:%M:%S %p %Z",
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        },
        secondary_log_colors={},
        style='%'
    )

    logger = logging.getLogger('Crawler')
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger
