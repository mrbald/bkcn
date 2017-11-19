#!/bin/env python3

import asyncio
import logging

from gw import bfx
from fw import log

tops = dict()
subs = list()

async def triangulate(loop = asyncio.get_event_loop()):

    def update(pair, st, _):
        global tops
        tops[pair] = st
        logging.info('update: %s', tops)
        if len(tops) is 3:
            eb = tops['ETHBTC']
            eu = tops['ETHUSD']
            bu = tops['BTCUSD']

            eb_d_bid = eb.b.p
            eb_d_ask = eb.a.p
            eb_xu_bid = eu.b.p / bu.a.p
            eb_xu_ask = eu.a.p / bu.b.p
            logging.info('============')
            logging.info('ETHBTC(direct):  bid: %14.12f, ask: %14.12f // %s',
                         eb_d_bid, eb_d_ask, 'good' if (eb_d_bid <= eb_d_ask) else 'crossed')
            logging.info('ETHBTC(via USD): ask: %14.12f, bid: %14.12f // X:%s (ETH:%s/BTC:%s)',
                         eb_xu_ask, eb_xu_bid,
                         'good' if (eb_xu_bid <= eb_xu_ask) else 'crossed',
                         'good' if (eu.b.p <= eu.a.p) else 'crossed',
                        'good' if (bu.b.p <= bu.a.p) else 'crossed')
            logging.info('------')

            eube_pxd = eb_d_bid - eb_xu_ask
            eube_qty = min(eu.b.q, eu.b.p * bu.a.q * bu.a.p, eb.a.q)
            logging.info('ETH(%.12f)->USD(%.12f)->BTC(%.12f)->ETH(%.12f): %.12f * %.12f = %.12f',
                         eu.b.q, bu.a.q * bu.a.p, eb.a.q * eb.a.p, eb.a.q,
                         eube_pxd, eube_qty, eube_pxd * eube_qty)

            ebue_pxd = eb_xu_bid - eb_d_ask
            ebue_qty = min(eb.b.q, eb.b.p * bu.a.q * bu.a.p, eb.a.q)
            logging.info('ETH(%.12f)->BTC(%.12f)->USD(%.12f)->ETH(%.12f): %.12f * %.12f = %.12f',
                         eb.b.q, bu.b.q, eu.a.q * eu.a.p, eu.a.q,
                         ebue_pxd, ebue_qty, ebue_pxd * ebue_qty)

    async with bfx.Gateway(uri='wss://api.bitfinex.com/ws/2', loop=loop) as gw:
        global subs
        #for pair in ('BTCUSD', 'ETHUSD', 'ETHBTC'):
        subs.append(await gw.sub('BTCUSD', qh = lambda st, evt: update('BTCUSD', st, evt)))
        subs.append(await gw.sub('ETHUSD', qh = lambda st, evt: update('ETHUSD', st, evt)))
        subs.append(await gw.sub('ETHBTC', qh = lambda st, evt: update('ETHBTC', st, evt)))
        while True:
            await asyncio.sleep(10, loop=loop)
            logging.info('tick')


def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(triangulate())


if __name__ == '__main__':
    log.configure()
    main()
