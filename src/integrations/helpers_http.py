import asyncio
import logging
from asyncio import Semaphore
from typing import Any, Dict, List, Optional

import httpx

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class AsyncHTTPClient:
    MAX_RETRIES = 30
    INITIAL_DELAY = 10

    def __init__(self, max_concurrency: int = 5, api_call_limit: int = 100):
        self.MAX_CONCURRENCY = max_concurrency
        self.api_call_counter = 0  # Initialize API call counter
        self.API_CALL_LIMIT = api_call_limit  # Set API call limit

    async def _request(
        self,
        method: str,
        sem: Semaphore,
        url: str,
        payload: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        retries: int = MAX_RETRIES,
        delay: int = INITIAL_DELAY,
    ):
        async with sem:
            async with httpx.AsyncClient() as client:
                for attempt in range(retries):
                    try:
                        response = await client.request(
                            method,
                            url,
                            params=payload,
                            headers=headers,
                            timeout=httpx.Timeout(15.0, connect=15.0, read=15.0),
                        )
                        response.raise_for_status()
                        self.api_call_counter += 1  # Increment API call counter
                        logging.info(
                            f"Successful request to {url} with status code {response.status_code}"
                        )
                        return {
                            "url": url,
                            "data": response.json(),
                            "status_code": response.status_code,
                        }

                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 429:
                            logging.warning(
                                f"Too Many Requests. Retrying in {delay} seconds..."
                            )
                            await asyncio.sleep(delay)
                            delay *= 2  # Exponential backoff
                        else:
                            logging.error(
                                f"Request failed with status code {exc.response.status_code}"
                            )
                            logging.error(f"Error message: {exc}")
                            return {
                                "status_code": exc.response.status_code,
                                "url": url,
                            }

                    except Exception as exc:
                        logging.error(f"An unexpected error occurred: {str(exc)}")
                        return {
                            "status_code": 0,
                            "url": url,
                        }

    async def get_all_responses(
        self,
        method: str,
        urls: List[str],
        payloads: Optional[List[Dict[str, Any]]] = None,
        headers: Optional[Dict[str, str]] = None,
    ):
        sem = Semaphore(self.MAX_CONCURRENCY)
        tasks = [
            self._request(method, sem, url, payload, headers=headers)
            for url in urls
            for payload in (payloads or [None])
        ]

        responses = []
        for future in asyncio.as_completed(tasks):
            if self.api_call_counter >= self.API_CALL_LIMIT:
                logging.warning(
                    f"API call limit reached ({self.API_CALL_LIMIT}). Pausing for a moment..."
                )
                await asyncio.sleep(50)
                self.api_call_counter = 0
            response = await future
            if response is not None:
                responses.append(response)
        return responses
