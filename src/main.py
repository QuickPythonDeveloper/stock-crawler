# -*- coding: utf-8 -*-

"""Crawler Google style docstring.

This module contains Optim Block behaviour with an external core_exchange.


`PEP 484`_ type annotations are supported. If attribute, parameter, and
return types are annotated according to `PEP 484`_, they do not need to be
included in the docstring:
    _PEP 484:
    https://www.python.org/dev/peps/pep-0484/
"""
from .crawler import Crawler


def run():
    """
    This is main method to run crawler async.
    run() method fire whole application up and running.
    """
    crawler = Crawler()
    crawler.run()


if __name__ == "__main__":
    run()
