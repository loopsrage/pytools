import asyncio
import re
import traceback

from pydantic_settings import BaseSettings

import playwright
from queue_controller.helpers import new_controller
from queue_controller.queueController import QueueController
from queue_controller.queueData import QueueData
from thread_safe.index import Index
from thread_safe.onceler import Onceler


class ScrapeUrls(BaseSettings):
    url: str
    selectors: list[str]
    filter: str

class PlaywrightSettings(BaseSettings):
    enabled: bool
    headless: bool
    scrape_urls: list[ScrapeUrls]
    interval: int
    start_now: bool


semaphore = asyncio.Semaphore(5)
async def download(fs, remote_dir, context, url):
    async with semaphore:
        page = await context.new_page()
        try:
            async with page.expect_download() as download_info:

                try:
                    await page.goto(url, wait_until="networkidle")
                except Exception as e:
                    if "ERR_ABORTED" in str(e) or "download" in str(e).lower():
                        pass
                    else:
                        raise e

            file = await download_info.value
            temp_path = await file.path()
            remote_path = f"{remote_dir}/{file.suggested_filename}"

            def process_upload():
                with open(temp_path, "rb") as f:
                    data = f.read()
                return fs.load_or_store(remote_path, lambda: data)

            _, was_loaded = await asyncio.to_thread(process_upload)
            if was_loaded:
                print(f"Skipped {remote_path} (MD5 Match)")
            else:
                print(f"Uploaded {remote_path} to {fs.filesystem}")
        except playwright._impl._errors.TimeoutError as e:
            traceback.print_exception(e)
        except Exception as e:
            traceback.print_exception(e)
            raise e
        finally:
            await page.close()

def page_node_storage(fs, remote_dir):
    async def page_node(queue_data: QueueData):
        page, scrape_args, page_data = queue_data.attributes("page", "scrape_args", "page_data")
        context = page.context
        try:
            valid_links = []
            pdf_links = await page.eval_on_selector_all(*scrape_args.selectors)
            for l in pdf_links:
                if re.search(scrape_args.filter, l):
                    valid_links.append(download(fs, remote_dir, context, l))
            await asyncio.gather(*valid_links)
        except Exception as e:
            raise e
        finally:
            await page.close()
    return new_controller(action=page_node)

class Playwright:
    _data = None
    _queue: asyncio.Queue = None
    p_node: QueueController
    onceler: Onceler
    def __init__(self,urls: ScrapeUrls, output: QueueController):
        self._data = Index()
        self._urls = urls
        self._queue = asyncio.Queue()
        self.p_node = output
        self.onceler = Onceler()

    async def worker(self, context):
        while True:
            scrape_args: ScrapeUrls = await self._queue.get()
            if scrape_args is None:
                self._queue.task_done()
                break
            try:
                async def init_page():
                    page = await context.new_page()
                    page_data = await page.goto(
                        scrape_args.url,
                        wait_until="networkidle")
                    return page, page_data
                page, page_data = await self.onceler.astore_once(scrape_args.url, "init_page_once", init_page)

                async def enqueue_once():
                    await self.p_node.enqueue(QueueData(
                        page=page,
                        scrape_args=scrape_args,
                        page_data=page_data,
                    ))
                await self.onceler.astore_once(scrape_args.url, "enqueue_once", enqueue_once)

            except Exception as e:
                print(e)
            finally:
                self._queue.task_done()

    async def run_loop(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()

            worker_task = asyncio.create_task(self.worker(context))

            await self._queue.put(*self._urls)

            await self._queue.join()
            await self.p_node.queue.join()

            await self._queue.put(None)
            await self.p_node.queue.put(None)
            await worker_task
            await browser.close()

