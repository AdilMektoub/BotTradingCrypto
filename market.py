# -*- coding: utf-8 -*-
__author__ = 'Adil'

import asyncio
import pickle
import datetime,time,json,...,sys
import rotate_log
import wallet

#print("Fichier de Log utilise: {} ".format(rotate_log.FileNameLog))

CONFIG = "config/config-exchanges.json"
logger = rotate_log.logger

ORDERS = list()

class PriceError(Exception):

    def __init__(self, price, quantity, *args):
        self.price = price
        self.qte   = quantity

        Exception.__init__(self, *args)

class Order:

    ACHAT  = 0
    VENTE  = 1
    CANCEL = 2

    def __init__(self, type, wallet, query_result, variables):
        self.type   = type
        self.vars   = variables
        self.wallet = wallet
        self.Id     = query_result['id']

    def end(self, order):

        if order == None:
            return None

        price = order['price']
        qte   = order['filled']

        result = {
            'vars' : self.vars,
            'order': order
        }

        return result

class MarketPlace:

    funcs = [
    #   Public calls            Private Calls
        'fetchMarkets',         'fetchBalance',
        'loadMarkets',          'createOrder',
        'fetchOrderBook',        'createLimitBuyOrder',
        'fetchStatus',          'createLimitSellOrder',
        'fetchL2OrderBook',     'createMarketBuyOrder',
        'fetchTrades',          'createMarketSellOrder',
        'fetchTicker',          'cancelOrder',
        'fetchTickers',         'fetchOrder',
                                'fetchOrders',
                                'fetchOpenOrders',
                                'fetchClosedOrders',
                                'fetchMyTrades'
    ]


    test = False

    def __init__(self, market_infos, test=False):

        self.wallets = dict()
        dev_prod     = 'dev' if test else 'prod'

        with open(CONFIG, "r") as f:
            config = json.load(f)

        self.exchange = market_infos['exchange']
        self.account  = market_infos['account']
        self.futures  = market_infos['futures']
        self.actions  = 0
        self.test     = test
        self.symbols  = config[self.exchange]
        self.limits   = config['limits']

        self.get_api(
            market_infos['apiKey'],
            market_infos['apiSecret'],
            market_infos['uid']
        )

        if self.api == None:
            raise Exception("Cannot connect to ...")

    def add_wallet(self, wallet_infos, test):
        symbol = wallet_infos['symbol']
        if not self.checksymbol(symbol):
            logger.critical("This symbol is not recognised or not allowed")
            return None

        if symbol in self.wallets:
            logger.warning("The wallet of the market {} is already loaded".format())
            return pickle.dumps(self.wallets[symbol])

        account_id = f"{self.exchange}-{self.account}"

        start_bal = self.get_symbol_balance(symbol)

        wl = wallet.Wallet.load_wallet(wallet_infos, account_id, test, start_bal)
        self.wallets[symbol] = wl
        return pickle.dumps(wl)

    def unload_wallet(self, symbol):

        if symbol not in self.wallets:
            logger.error("Unable to unload a wallet not loaded (symbol: {})".format(symbol))
            return False

        self.wallets[symbol].write_wallet()
        del self.wallets[symbol]
        return True

    def write_wallets(self):
        for wallet in self.wallets.values():
            wallet.write_wallet()

    def ..._main(self, func, *args):
        """
            The base function of this linear api
            All calls to the api walk through this try/catch anyway.
            Before the call, we check if the api exist, if the function exist,
            and if the api is initialized.
        """
        if self.api and func in self.funcs and hasattr(self.api, func):
            try:
                method = getattr(self.api, func)
                return method(*args)
            except ....NetworkError as e:
                logger.error("... Network Error: {}".format(repr(e)))
                logger.info("Essai 1/2: Failed")
                try:
                    res = method(*args)
                    logger.info("Essai 2/2: Passed")
                    return res
                except Exception as err:
                    logger.error("Erreur %s exchange:%s erreur:%s " % (func, self.exchange, repr(err)))
                    return None
            except Exception as e:
                logger.error("Erreur %s exchange:%s erreur:%s " % (func, self.exchange, repr(e)))
                return None
        else:
            logger.error("You are trying to call a function that either not exist or \
                the api is not initialized or the connection to the platform is corrupted")
            return None

    def get_base_quote(self, symbol):

        i = symbol.find('/')

        if i == -1:
            logger.critical("Unable to get the self.base and the self.quote for this symbol")

        base  = symbol[:i]
        quote = symbol[i+1:]

        return (base, quote)

    async def check_finished(self, task, symbol):
        order = self. ..._main('fetchOrder', task.Id, symbol)
        while order['status'] == 'open':
            await asyncio.sleep(10)
            order = self. ..._main('fetchOrder', task.Id, symbol)

        lot     = task.end(order)
        minimal = self.get_minimal_quantity(symbol)
        lot['minimal'] = minimal

        self.wallets[symbol].manage_order(lot)
        ORDERS.append(lot)

    def stop_not_finished(self, symbol):
        wallet = self.wallets[symbol]
        for task in wallet.tasks.values():
            order = self. ..._main('fetchOrder', task.Id, symbol)
            if order['status'] == 'open':
                order = self.cancelOrder(task.Id, symbol)

        wallet.tasks = dict()

    def is_order_completed(self, Id, symbol):
        """ Check of an order is completed or if he is open """
        order = self. ..._main('fetchOrder', Id, symbol)
        return order['status'] != 'open'

    def get_limitsForMarkets(self, symbol):
        """return all the limits if a market like the minimal quantity of a transaction"""
        markets = self. ..._main('fetchMarkets')

        if markets:
            for market in markets:
                if 'symbol' in market and market['symbol'] == symbol and 'limits' in market:
                        return market['limits']

        logger.error("Cannot get Markets'limits: either this market hasn't or the symbol is not recognised.")

        return None

    def get_minimal_quantity(self, symbol):
        """return the minimal quantity to trade of a certain market"""
        wallet = self.wallets[symbol]
        if wallet.base in self.limits:
            return self.limits[wallet.base]
        else:
            logger.error("Failed to get minimal")
            return -1

    def checksymbol(self, symbol):
        """check if we allowed this marketplace to trade and if the api knows it"""
        markets = self. ..._main('fetchMarkets')

        if markets == None:
            logger.error("Cannot know if the symbol {} is supported".format(symbol))
            return False

        i = symbol.find('/')
        if i == -1:
            logger.error("The symbol {} seems invalid".format(symbol))
            return False

        base, quote = symbol.split('/')

        symbols = [symbol]

        if quote == "USDT":
            symbols.append("{}/{}".format(base, "USD"))

        for market in markets:
            if market['symbol'] in symbols:
                return symbol in self.symbols

        return False

    def get_symbol_list(self):
        markets = self. ..._main('fetchMarkets')

        if markets == None:
            logger.error("Cannot get the symbol list")
            return None

        return [market['symbol'] for market in markets]

    def get_api(self, apiKey, apiSecret, uid):

        params = {
            'apiKey': apiKey,
            'secret': apiSecret,
            'uid': uid,
            #'verbose': True,
            'adjustForTimeDifference': True,
            'recvWindow': 10000000
        }

        if self.futures:
            params['options'] = {'defaultMarket': 'futures'}

        method   = getattr(..., self.exchange)  # conversion string en method
        self.api = method(params)

        if not self. ..._main('loadMarkets'):
            logger.critical("Unable to load markets of the first time, abort")
            raise Exception("Unable to load markets of the first time, abort")

    def get_prix(self, symbol):

        tick = self. ..._main('fetchTicker', symbol)
        if isinstance(tick, dict):
            self.last   = tick['last']
            self.bid    = tick['bid']
            self.ask    = tick['ask']
            return True
        else:
            logger.error("Unable to get price from the platform")
            return False

    def get_symbol_balance(self, symbol):
        # import pdb; pdb.set_trace()
        balance = self. ..._main('fetchBalance')
        if balance and isinstance(balance, str) :
            try:
                balance = json.loads(balance)
            except Exception as e:
                logger.error("Unable to load balance result Balance: %s Erreur:%s " % (str(balance), e))
                return None

        if not isinstance(balance, dict) or ('status' in balance and balance['status'] == 'error'):
            logger.error("Error fetching the balance : {}".format(balance))
            return None

        base, quote = self.get_base_quote(symbol)

        if balance:
            if base in balance and quote in balance:
                return {base : balance[base]['free'], quote : balance[quote]['free']}
            elif base in balance:
                return {base: balance[base]}

            elif symbol[0] != '.' and base == 'XBT':
                return {'BTC': balance['BTC']['free']}

            elif symbol[0] != '.' and base != 'XBT' and base in balance:
                return {base: balance[base]['free']}
            else:
                logger.error("Unable To understand the balance format")
                return None
        else:
            logger.error("Unable to get balance of {}".format(symbol))
            return None


    def fetchTicker(self, symbol):
        ticker = self. ..._main('fetchTicker', symbol)
        if not ticker:
            tickers = self. ..._main('fetchTickers')
            if symbol in tickers:
                ticker = tickers[symbol]
            else:
                logger.error("Unable to get ticks")
                ticker = None

        return ticker


    # Fonction Vente
    ####################
    async def limit_sell(self, symbol, quantity, price, variables=None):
        self.actions += 1
        wallet = self.wallets[symbol]
        self.get_prix(symbol)
        logger.info("Try to Sell Symbol: %s Price: %s Quantite: %s" % (
            symbol, price, quantity))

        if quantity <= 0 or price <= 0:
            raise PriceError(price, quantity, "quantity or price is negative or null")

        try:
            result = self.api.createLimitSellOrder(symbol, quantity, price)
        except  ....InsufficientFunds as e:
            logger.error("insufficient Funds error: {}".format(repr(e)))
            minimal = self.get_minimal_quantity(symbol)
            balance = self.get_symbol_balance(symbol)

            if wallet.base not in balance:
                return None

            if balance[wallet.base] <= minimal:
                logger.error("Unable to buy there is less than the smallest quantity tradable on your account")
                return None

            try:
                result = self.api.createLimitSellOrder(symbol, balance[wallet.base], price)
            except Exception as e:
                logger.error("Error while buying: {}".format(repr(e)))
                return None

        except Exception as e:
            logger.error("Error while buying: {}".format(repr(e)))
            return None

        if not result or ('status' in result and result['status'] == 'error'):
            logger.error("An error occured while buying")
            return None

        logger.info(f"Order passed, ID = {result['id']}")

        variables  = variables or dict()
        task       = Order(Order.VENTE, wallet, result, variables)
        task.ended = asyncio.create_task(self.check_finished(task, symbol))

        wallet.tasks[task.Id] = task

        return result

    #Buy Order
    ####################################
    async def limit_buy(self, symbol, quantity, price, variables=None):
        self.actions += 1
        wallet        = self.wallets[symbol]
        self.get_prix(symbol)
        logger.info("Try to buy Symbol: %s Price: %s Quantite: %s" % (
            symbol, price, quantity))

        if quantity <= 0 or price <= 0:
            raise PriceError(price, quantity, "quantity or price is negative or null")

        if not self.api:
            logger.error("The api seems to not be initialized")
            return None

        try:
            result = self.api.createLimitBuyOrder(symbol, quantity, price)
        except  ....InsufficientFunds as e:
            logger.error("insufficient Funds error: {}".format(repr(e)))
            minimal = self.get_minimal_quantity(symbol)
            balance = self.get_symbol_balance(symbol)

            if wallet.quote not in balance:
                return None

            if balance[wallet.quote] / price <= minimal:
                logger.error("Unable to buy there is less than the smallest quantity tradable on your account")
                return None

            try:
                result = self.api.createLimitBuyOrder(symbol, balance[wallet.quote] / price, price)
            except Exception as e:
                logger.error("Error while buying: {}".format(repr(e)))
                return None

        except Exception as e:
            logger.error("Error while buying: {}".format(repr(e)))
            return None

        if not result or ('status' in result and result['status'] == 'error'):
            logger.error("An error occured while buying")
            return None

        logger.info(f"Order passed, ID = {result['id']}")

        variables  = variables or dict()
        task       = Order(Order.ACHAT, wallet, result, variables)
        task.ended = asyncio.create_task(self.check_finished(task, symbol))

        wallet.tasks[task.Id] = task
        return result

    def cancelOrder(self, Id, symbol):
        order = self. ..._main('cancelOrder', Id, symbol)

        if not order or ('error' in order and len(order['error']) != 0):
            logger.error("Cannot cancel order of Id {} and symbol {}".format(Id, symbol))
            return None

        return order

    def fetchOpenOrders(self, symbol=""):
        if symbol == "":
            orders = self. ..._main('fetchOpenOrders')
        else:
            orders = self. ..._main('fetchOpenOrders', symbol)

        if not orders:
            logger.error("Cannot gather open orders")
            return None

        return orders
