import argparse
import asyncio
import pprint
import time

import aiohttp
import requests
import yaml


def parse_options():
    parser = argparse.ArgumentParser(description='Fetch some urls')
    parser.add_argument('-c', '--configuration-file', dest='config_file', action='store',
                        default='fetch-config.yaml',
                        help='Repeatedly fetch urls from a config')
    return parser.parse_args()


def read_config(filename):
    with open(filename) as cf:
        return yaml.safe_load(cf)


class Fetcher:
    def __init__(self, config, loop):
        self.config = config
        self.loop = loop

    async def run(self):
        if self.config.mode == 'async':
            return await asyncio.gather(*[self.fetch_session(p, v) for p, v in self.config.sessions.items()])
        elif self.config.mode == 'requests':
            results = []
            for prefix, urls in self.config.urls.items():
                for url in urls:
                    results.append(requests.get(f'{prefix}/{url}'))
            return results

    async def fetch_session(self, prefix, session_config):
        urls = session_config.get('urls', [])
        max_connections = session_config.get('max-parallell-requests', 1000)
        targets = [f'{prefix}/{url}'for url in urls]
        async with aiohttp.ClientSession() as session:
            sem = asyncio.Semaphore(max_connections)
            return await asyncio.gather(*[self.limited_fetch_url(target, session, sem) for target in targets])

    async def limited_fetch_url(self, target, session, sem):
        async with sem:
            return await self.fetch_url(target, session)

    async def fetch_url(self, url, session):
        t1 = time.time()
        async with session.get(url) as response:
            content = await response.text()
            return {
                'url': url,
                'size': len(content),
                'status': response.status,
                'dt': time.time() - t1
            }


if __name__ == '__main__':
    options = parse_options()
    fileconfig = read_config(options.config_file)
    options.mode = fileconfig.get('mode', 'async')
    options.sessions = fileconfig.get('sessions', [])
    loop = asyncio.get_event_loop()
    fetcher = Fetcher(options, loop)
    res = loop.run_until_complete(fetcher.run())
    pprint.pprint(res)
    loop.close()
