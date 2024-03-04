from email import header
import httpx
import asyncio
from typing import Dict, Any, List, Optional, Union
from asyncio import Semaphore


class AsyncHTTPClient:
    MAX_RETRIES = 30
    INITIAL_DELAY = 10

    def __init__(self, url_base: str, max_concurrency: int = 1):
        self.URL_BASE = url_base
        self.MAX_CONCURRENCY = max_concurrency

    def _create_url(self, ext_url: str) -> str:
        if ext_url:
            return self.URL_BASE + ext_url
        else:
            return self.URL_BASE

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
                        output = response.json()
                        output["payload"] = payload or {}
                        output["payload"]["status_code"] = response.status_code
                        return output

                    except httpx.HTTPStatusError as exc:
                        if exc.response.status_code == 429:
                            print(f"Too Many Requests. Retrying in {delay} seconds...")
                            await asyncio.sleep(delay)
                            delay *= 2  # Exponential backoff

                        else:
                            print(
                                f"Request failed with status code {exc.response.status_code}"
                            )
                            print(f"Error message: {exc}")
                            output = {}
                            output["payload"] = payload or {}
                            output["payload"]["status_code"] = exc.response.status_code

                            return output

                    except Exception as exc:
                        print(f"An unexpected error occurred: {str(exc)}")
                        output = {}
                        output["payload"] = payload or {}
                        output["payload"]["status_code"] = 0
                        return output

    async def get_all_responses(
        self,
        method: str,
        urls,
        payloads: list[Dict[str, Any]],
        headers: Optional[Dict[str, str]] = None,
    ):
        sem = Semaphore(self.MAX_CONCURRENCY)
        tasks = []
        for url in urls:
            for payload in payloads:
                task = self._request(method, sem, url, payload, headers=headers)
                tasks.append(task)
        responses = await asyncio.gather(*tasks)

        filtered_responses = [r for r in responses if r is not None]
        return filtered_responses
