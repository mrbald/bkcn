import asyncio
import logging

from gw import bmx
from fw import log

subs = list()


async def sandbox(loop=asyncio.get_event_loop()):
    async with bmx.Gateway(loop=loop) as gw:
        await gw.prime('XBTUSD')
        logging.info("sleeping")
        global subs
        #for pair in ('BTCUSD', 'ETHUSD', 'ETHBTC'):
        #subs.append(await gw.sub('XBTUSD', qh = lambda st, evt: print(st, evt)))
        while True:
            await asyncio.sleep(10, loop=loop)
            logging.info('tick')

    logging.info("test coroutine done inside")


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(sandbox())


if __name__ == '__main__':
    log.configure()
    main()
