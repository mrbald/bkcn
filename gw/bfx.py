'''
Copyright 2017 Vladimir Lysyy (mrbald@github)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

"""
A web-socket connector to bitfinex.com

API docs: https://bitfinex.readme.io/v2/docs/ws-general/
Coroutines spec: https://www.python.org/dev/peps/pep-0492/
Coroutines tutorial: http://stackabuse.com/python-async-await-tutorial/
"""

import asyncio
import websockets
import logging
import json
import time

from collections import namedtuple, defaultdict
from recordclass import recordclass

'''
class Subscription:
    def __init__(self, gw, symbol, type):
        self.logger = logging.getLogger(symbol)
        self.queue = asyncio.Queue(loop=gw.loop)
        self.gw = gw
        self.symbol = symbol
        self.type = type

    async def __aenter__(self):
        await self.gw.send({
            'event': 'subscribe',
            'channel': 'ticker',
            'symbol': self.symbol
        })

        resp = await asyncio.wait_for(self.queue.get(), time=5)
        if resp:
            self.chanId = resp['chanId']
        else:
            raise RuntimeError('invalid subscription response')

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.gw.send({
            'event': 'unsubscribe',
            'chanId': self.chanId
        })

        self.sock = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.queue.get()

    async def dispatch(self, msg):
        await self.queue.put(msg)
'''

PerKind = recordclass('PerKind', ('ticker', 'trades', 'book'))
PrimeRec = recordclass('PrimeRec', ('ids', 'subs'))
HandlerRec = recordclass('HandlingRec', ('handler', 'state', 'listeners'))

sq = namedtuple('sq', ('t', 'p', 'q'))  # side quote (bid or ask)
fq = namedtuple('fq', ('b', 'a'))  # full quote (bid + ask)
tr = namedtuple('tr', ('t', 'p', 'q'))  # trade

class Gateway:
    ping = {'event': 'ping'}
    sock = None
    primed = defaultdict(PrimeRec)
    handlers = defaultdict(HandlerRec)

    def __init__(self, uri, loop = asyncio.get_event_loop(), tob=True, trd=True, dob=False):
        assert tob or trd or dob

        self.logger = logging.getLogger(uri)
        self.uri = uri
        self.loop = loop
        self.tob = tob
        self.trd = trd
        self.dob = dob

    async def start(self):
        self.logger.info('connecting')
        self.sock = await websockets.connect(uri=self.uri, loop=self.loop)

        greeting = await self.__recv()
        logging.info('connected %s', greeting)

    async def ping(self):
        await self.__send(self.ping)
        pong = await self.__recv()
        logging.info('received %s', pong)

    async def stop(self):
        if self.sock:
            self.logger.info('disconnecting')
            await self.sock.close()
            self.sock = None
            self.logger.info('disconnected')

    async def __aenter__(self):
        await self.start()
        asyncio.ensure_future(self.__dispatch(), loop=self.loop)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def prime(self, symbol):
        if symbol not in self.primed:
            topic = 't%s' % symbol
            self.logger.info("priming %s (%s)", symbol, topic)

            self.primed[symbol] = PrimeRec(ids=PerKind(None, None, None), subs=PerKind(list(),list(),list()))

            if self.tob:
                chan = 'ticker'
                self.logger.info("priming top of book %s (%s:%s)", symbol, chan, topic)
                await self.__send({'event': 'subscribe', 'channel': chan, 'symbol': topic})

            if self.trd:
                chan = 'trades'
                self.logger.info("priming %s (%s:%s)", symbol, chan, topic)
                await self.__send({'event': 'subscribe', 'channel': chan, 'symbol': topic})

            if self.dob:
                chan = 'book'
                self.logger.info("priming depth of book %s (%s:%s)", symbol, chan, topic)
                await self.__send({'event': 'subscribe', 'channel': chan, 'symbol': topic, 'prec': 'P0', 'freq': 'F0', 'len': 25})

    async def __send(self, msg):
        return await self.sock.send(json.dumps(msg))

    async def __recv(self):
        try:
            return json.loads(await self.sock.recv())
        except websockets.ConnectionClosed:
            return None

    async def sub(self, symbol, qh=None, th=None, bh=None):
        # TODO: VL: initial snapshot

        if qh or th or bh:
            self.logger.info('subscribing to %s', symbol)
            if symbol not in self.primed:
                await self.prime(symbol)

            subs = self.primed[symbol].subs

            def place(lst, val):
                try:
                    pos = lst.index(None)
                except ValueError:
                    pos = len(lst)
                    lst.append(None)
                lst[pos] = val
                return pos

            qp = place(subs.ticker, qh) if qh else None
            tp = place(subs.trades, th) if th else None
            bp = place(subs.book, bh) if bh else None

            return symbol, PerKind(qp, tp, bp)
        else:
            return None

    def drop(self, key):
        self.logger.info('dropping %s', key)
        symbol, ids = key
        rec = self.primed[symbol]
        if ids.ticker is not None:
            rec.subs.ticker[ids.ticker] = None
        if ids.trades is not None:
            rec.subs.trades[ids.trades] = None
        if ids.book is not None:
            rec.subs.book[ids.book] = None
        # TODO: VL: reference counted venue side sub/drop

    @staticmethod
    def stamp():
        return int(time.time() * 1e6)

    def __make_handler(self, pair, kind):
        if kind == 'ticker':
            def handler(st, msg, listeners):
                now = self.stamp()
                bid, bid_sz, ask, ask_sz, _, _, _, _, _, _ = msg[1]
                self.logger.debug('TICK %s: %g@%gx%g@%g', pair, bid_sz, bid, ask_sz, ask)

                xxx = fq(sq(now, bid, bid_sz), sq(now, ask, ask_sz))

                if st is None:
                    next_st = xxx
                    evt = next_st
                elif st.b == xxx.b and st.a == xxx.a:
                    next_st = st
                    evt = fq(None, None)
                elif st.b != xxx.b:
                    next_st = fq(xxx.b, st.a)
                    evt = fq(xxx.b, None)
                else:
                    next_st = (st.b, xxx.a)
                    evt = fq(None, xxx.a)

                for listener in listeners:
                    if listener:
                        listener(next_st, evt)

                return next_st

        elif kind == 'trades':
            def handler(st, msg, listeners):
                now = self.stamp()
                leg = msg[1]
                if leg == 'te':
                    _, _, qty, px = msg[2]
                    self.logger.debug('TRADE %s: %g@%g', pair, qty, px)
                    next_st = evt = tr(now, px, qty)
                    for listener in listeners:
                        if listener:
                            listener(next_st, evt)
                    return next_st

        elif kind == 'book':
            bids = dict()
            asks = dict()

            def handler(state, msg, listeners):
                if isinstance(msg[1][0], list):  # snapshot
                    for x in msg[1]:
                        px, _, vol = x
                        self.logger.debug('BOOK %s: %g@%g', pair, vol, px)
                        if vol > 0:
                            bids[px] = vol
                        elif vol < 0:
                            asks[px] = -vol
                        else:
                            bids.pop(px, None)
                            asks.pop(px, None)

                else:  # update
                    px, _, vol = msg[1]
                    self.logger.debug('BOOK %s: %g@%g', pair, vol, px)
                    if vol > 0:
                        bids[px] = vol
                    elif vol < 0:
                        asks[px] = -vol
                    else:
                        bids.pop(px, None)
                        asks.pop(px, None)

                logging.info('BOOK %s: \n%s\n%s', pair, sorted(bids.keys()), sorted(asks.keys()))
                return None
        else:
            self.logger.warning('unexpected subscription kind in response: %s', kind)
            def handler(st, evt, listeners):
                pass

        return handler

    def __subscribed(self, resp):
        chan_id = resp['chanId']
        kind = resp['channel']
        pair = resp['pair']

        prime_rec = self.primed[pair]
        setattr(prime_rec.ids, kind, chan_id)

        self.handlers[chan_id] = HandlerRec(
            self.__make_handler(pair, kind),
            None,
            getattr(prime_rec.subs, kind)
        )

    async def __dispatch(self):
        while True:
            resp = await self.__recv()
            if not resp:
                self.logger.info("reader terminated")
                break;

            if isinstance(resp, list):
                if resp[1] != 'hb':
                    chan_id = resp[0]
                    handler_rec = self.handlers[chan_id]
                    handler_rec.state = handler_rec.handler(handler_rec.state, resp, handler_rec.listeners)
            elif isinstance(resp, dict):
                if 'event' in resp and resp['event'] == 'subscribed':
                    self.__subscribed(resp)
                else:
                    self.logger.warning('unexpected response: %s', resp)
                logging.info('map: %s', resp)
