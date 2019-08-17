import argparse
import logging
import time
from typing import Text
import aiohttp

import grpc
from proto import scraping_pb2, scraping_pb2_async_grpc


async def download_web_page(link: Text):
    """Download the HTML for the specific link."""
    logging.info('Downloading [%s]', link)
    async with aiohttp.ClientSession() as session:
        async with session.get(link) as response:
            return await response.text()


class Scrapper(object):

    async def scrape(self,
                     stream: scraping_pb2_async_grpc.ScrapperScrapeStream):
        while True:
            request = await stream.receive()
            if request is None:
                return
            try:
                await stream.send(
                    scraping_pb2.ScrapingResponse(page=scraping_pb2.WebPage(
                        url=request.target,
                        content=await download_web_page(request.target),
                    ))
                )
            except Exception as e:
                logging.error(
                    'Failed to parse [%s] due to [%s]',
                    request.target, e
                )


def main():
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument('--port', default=":10000", nargs=1,
                        type=str, help='The port to listen on')
    args = parser.parse_args()

    server = grpc.server()
    port = server.add_insecure_port(args.port)

    scraping_pb2_async_grpc.add_ScraperServicer_to_server(Scrapper(), server)

    server.start()
    logging.info('Scraper server start at :%d', port)

    try:
        time.sleep(86400)
    except InterruptedError:
        pass
    finally:
        server.stop(None)


if __name__ == "__main__":
    main()
