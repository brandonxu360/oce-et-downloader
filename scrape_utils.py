import requests
import aiohttp
import asyncio
import logging
from requests.exceptions import HTTPError, Timeout, ConnectionError
from aiohttp import ClientResponseError, ClientConnectionError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_log, after_log

logger = logging.getLogger('climateengine.http')

@retry(
    before=before_log(logger, logging.DEBUG),
    after=after_log(logger, logging.DEBUG),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((HTTPError, Timeout, ConnectionError)),
    reraise=True
)
def synchronous_fetch_with_retry(url: str, **kwargs):
    response = requests.get(url, **kwargs)
    response.raise_for_status()

    return response.json()

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((ClientResponseError, ClientConnectionError)),
    reraise=True
)
async def asynchronous_fetch_with_retry(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore, **kwargs):
    """
    Asynchronously fetch data from a URL with automatic retry logic using tenacity and aiohttp. You must 
    pass a client session that has raise_for_status set to True for the retries to work.
    """
    
    async with semaphore:
        @retry(
            before=before_log(logger, logging.DEBUG),
            after=after_log(logger, logging.DEBUG),
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=1, max=10),
            retry=retry_if_exception_type((ClientResponseError, ClientConnectionError)),
            reraise=True
        )
        async def _fetch():
            async with session.get(url, **kwargs) as response:
                result = await response.json()
                return result
        
        return await _fetch()
