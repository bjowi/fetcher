import argparse
import asyncio
import pprint
import signal
import sys
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


def get_time_span(timings):
    resolution = timings.get('resolution', 240)
    span = timings.get('span', 3600) // resolution
    end = int(time.time() // resolution) * resolution
    begin = end - (span * resolution)
    return begin, end


class Fetcher:
    def __init__(self, config, loop):
        self.config = config
        self.loop = loop
        self.result_queue = asyncio.Queue()
        self.stop_event = asyncio.Event()

    async def run(self):
        if self.config.mode == 'async':
            return await asyncio.gather(*[self.fetch_session(s) for s in self.config.sessions])
        elif self.config.mode == 'requests':
            results = []
            for prefix, urls in self.config.urls.items():
                for url in urls:
                    results.append(requests.get(f'{prefix}/{url}'))
            return results

    async def shutdown(self, sig):
        self.stopevent.set()
        print(f"Shutting down due to signal {sig.name}")

    async def fetch_session(self, session_config):
        prefix = session_config['prefix']
        urls = session_config.get('urls', [])
        max_connections = session_config.get('max-parallel-requests', 1000)
        targets = [f'{prefix}/{url}'for url in urls]
        args = {}
        timings = session_config.get('timings')
        period = 300
        if timings:
            args['begin'], args['end'] = get_time_span(timings)
            period = timings.get('period', period)

        while not self.stop_event.is_set():
            async with aiohttp.ClientSession() as session:
                sem = asyncio.Semaphore(max_connections)
                results = await asyncio.gather(*[self.limited_fetch_url(target, args, session, sem)
                                                 for target in targets])
                await self.result_queue.put(results)
                await asyncio.sleep(period)
                print(f'slept {period}s')

    async def limited_fetch_url(self, target, args, session, sem):
        async with sem:
            return await self.fetch_url(target, args, session)

    async def fetch_url(self, url, args, session):
        print(args)
        t1 = time.time()
        async with session.get(url, params=args) as response:
            content = await response.text()
            return {
                'url': url,
                'size': len(content),
                'status': response.status,
                'dt': time.time() - t1
            }


if __name__ == '__main__':
    print(sys.platform)
    options = parse_options()
    fileconfig = read_config(options.config_file)
    options.mode = fileconfig.get('mode', 'async')
    options.sessions = fileconfig.get('sessions', [])
    loop = asyncio.get_event_loop()
    fetcher = Fetcher(options, loop)

    if sys.platform != 'win32':
        signals = [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]
        for s in signals:
            loop.add_signal_handler(s, lambda s=s: asyncio.ensure_future(fetcher.shutdown(s)))
    else:
        print("Running in windows, stopping is not implemented")
        print()
        print('bah')

    res = loop.run_until_complete(fetcher.run())
    pprint.pprint(res)
    loop.close()
