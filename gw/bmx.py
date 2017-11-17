"""
A web-socket connector to bitmex.com

API docs: https://www.bitmex.com/app/wsAPI/
Coroutines spec: https://www.python.org/dev/peps/pep-0492/
Coroutines tutorial: http://stackabuse.com/python-async-await-tutorial/
"""

import asyncio
import websockets
import logging
import json
import time
import dateutil.parser

from collections import namedtuple, defaultdict
from recordclass import recordclass

PerKind = recordclass('PerKind', ('quote', 'trade', 'orderBookL2'))
PrimeRec = recordclass('PrimeRec', ('ids', 'subs'))
HandlerRec = recordclass('HandlingRec', ('handler', 'state', 'listeners'))

sq = namedtuple('sq', ('t', 'p', 'q'))  # side quote (bid or ask)
fq = namedtuple('fq', ('b', 'a'))  # full quote (bid + ask)
tr = namedtuple('tr', ('t', 'p', 'q'))  # trade

class Gateway:
    id = 'BMX'
    ping = {'event': 'ping'}
    sock = None
    primed = defaultdict(PrimeRec)
    handlers = defaultdict(HandlerRec)

    def __init__(self, uri = 'wss://www.bitmex.com/realtime', loop = asyncio.get_event_loop(), tob=True, trd=True, dob=False):
        assert tob or trd or dob

        self.logger = logging.getLogger(self.id)
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
        self.dispatcher = self.__dispatch()
        asyncio.ensure_future(self.dispatcher, loop=self.loop)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    async def prime(self, symbol):
        if symbol not in self.primed:
            topic = symbol
            self.logger.info("priming %s (%s)", symbol, topic)

            self.primed[symbol] = PrimeRec(ids=PerKind(None, None, None), subs=PerKind(list(),list(),list()))

            commands = list()
            request = {'op': 'subscribe', 'args': commands}

            if self.tob:
                chan = 'quote'
                self.logger.info("priming top of book %s (%s:%s)", symbol, chan, topic)
                commands.append('{}:{}'.format(chan, symbol))

            if self.trd:
                chan = 'trade'
                self.logger.info("priming %s (%s:%s)", symbol, chan, topic)
                commands.append('{}:{}'.format(chan, symbol))

            if self.dob:
                chan = 'orderBookL2'
                self.logger.info("priming depth of book %s (%s:%s)", symbol, chan, topic)
                commands.append('{}:{}'.format(chan, symbol))

            await self.__send(request)

    async def __send(self, msg):
        return await self.sock.send(json.dumps(msg))

    async def __recv(self):
        try:
            return json.loads(await self.sock.recv())
        except websockets.ConnectionClosed:
            return None

    async def sub(self, symbol, qh=None, th=None, bh=None):
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

            qp = place(subs.quote, qh) if qh else None
            tp = place(subs.trade, th) if th else None
            bp = place(subs.orderBookL2, bh) if bh else None

            return symbol, PerKind(qp, tp, bp)
        else:
            return None

    def drop(self, key):
        self.logger.info('dropping %s', key)
        symbol, ids = key
        rec = self.primed[symbol]
        if ids.quote is not None:
            rec.subs.quote[ids.quote] = None
        if ids.trade is not None:
            rec.subs.trade[ids.trade] = None
        if ids.orderBookL2 is not None:
            rec.subs.orderBookL2[ids.orderBookL2] = None
        # TODO: VL: reference counted venue side sub/drop

    @staticmethod
    def stamp():
        return int(time.time() * 1e6)

    def __make_handler(self, pair, kind):
        if kind == 'quote':
            def handler(st, msg, listeners):
                now = int(dateutil.parser.parse(msg['timestamp']).timestamp() * 1e6)
                xxx = fq(sq(now, float(msg['bidPrice']), float(msg['bidSize'])),
                         sq(now, float(msg['askPrice']), float(msg['askSize'])))
                self.logger.info(xxx)
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

        elif kind == 'trade':
            def handler(st, msg, listeners):
                now = int(dateutil.parser.parse(msg['timestamp']).timestamp() * 1e6)

                next_st = evt = tr(now, float(msg['size']), float(msg['price']))
                for listener in listeners:
                    if listener:
                        listener(next_st, evt)
                return next_st

        elif kind == 'orderBookL2':
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

    def __subscribed(self, chan_id, kind, pair):
        self.logger.info('subscribed to %s', chan_id)

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

            if isinstance(resp, dict):
                logging.info('map: %s', resp)
                if 'table' in resp:
                    kind = resp['table']
                    if 'data' in resp:
                        for msg in resp['data']:
                            if 'symbol' in msg:
                                symbol = msg['symbol']
                                chan_id = '{}:{}'.format(kind, symbol);
                                self.logger.info('looking up %s', chan_id)
                                if chan_id not in self.handlers:
                                    self.logger.info('not found, registering subscription %s', chan_id)
                                    self.__subscribed(chan_id, kind=kind, pair=symbol)
                                handler_rec = self.handlers[chan_id]
                                handler_rec.state = handler_rec.handler(handler_rec.state, msg, handler_rec.listeners)
                elif 'success' in resp:
                    if bool(resp['success']):
                        if chan_id not in self.handlers:
                            chan_id = resp['subscribe']
                            kind, pair = chan_id.split(':')
                            self.__subscribed(chan_id, kind=kind, pair=pair)
                    else:
                        self.logger.warning('subscription failed: %s', resp)
                else:
                    self.logger.warning('unexpected response: %s', resp)
