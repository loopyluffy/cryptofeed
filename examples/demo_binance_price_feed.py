

# import pandas as pd
from datetime import datetime
from cryptofeed import FeedHandler
from cryptofeed.defines import CANDLES
from cryptofeed.exchanges import Binance, BinanceFutures, BinanceDelivery
# from cryptofeed.symbols import Symbol, Symbols
# from cryptofeed.exchange import Exchange

class BinancePriceCompare:
    # def __init__(self, exchange: Exchange, quote_asset) -> None:
    def __init__(self, quote_asset, base_assets: list=None) -> None:
        # self.exchange = exchange
        self.base_assets = base_assets
        self.quote_asset = quote_asset

        binance_info = Binance.info()
        binance_futures_info = BinanceFutures.info()
        # binance_inverse_info = BinanceDelivery.info()

        # print(BINANCE_FUTURES)
        # print(binance_futures_info)
        # print(Symbols.get(BINANCE_FUTURES))

        # Step 1: Filter out only quote asset market for both spot and futures
        self.quote_spot_family = []
        self.quote_futures_family = []
        if base_assets is None:
            self.quote_spot_family = [s for s in binance_info["symbols"] if f"-{self.quote_asset}" in s.lower()]
            self.quote_futures_family = [s for s in binance_futures_info["symbols"] if f"-{self.quote_asset}" in s.lower() and "pindex" not in s.lower()]
        else:
            for base in self.base_assets:
                self.quote_spot_family.extend([s for s in binance_info["symbols"] if f"{base}-{self.quote_asset}" in s.lower()])
                self.quote_futures_family.extend([s for s in binance_futures_info["symbols"] if f"{base}-{self.quote_asset}" in s.lower() and "pindex" not in s.lower()])
        
        # print(f"binance symbols: -{self.quote_asset} family ----------------------------")
        # print(self.quote_spot_family)
        # print(self.quote_futures_family)

        # Step 2: Create a mapping from spot to multiple futures symbols
        self.symbol_mapping = {}
        for spot_symbol in self.quote_spot_family:
            # Find all corresponding futures symbols containing the base symbol
            matched_futures = [futures for futures in self.quote_futures_family if spot_symbol in futures]
            # Store the list of futures symbols in the dictionary, mapped to the spot symbol
            self.symbol_mapping[spot_symbol] = matched_futures

        self.spot_price = {}
        self.futures_price = {}

    @property
    def spot_symbols(self):
        # for spot_symbol in self.quote_spot_family:
        #     return []
        return self.quote_spot_family
    
    @property
    def futures_symbols(self):
        return self.quote_futures_family
    
    # @property
    # def spot_futures_mapping(self):
    #     return self.symbol_mapping
        
    async def candles(self, candles, receipt_timestamp):
        print("--------------------------------------------------------")
        print(f'Candle received at {datetime.fromtimestamp(receipt_timestamp)}: {candles.exchange} {candles.symbol} {candles.close}')
        # print(f'Candle received at {pd.to_datetime(receipt_timestamp)}: {candles.exchange} {candles.symbol} {candles.close}')
        current_spot = None
        if candles.symbol in self.quote_spot_family:
            self.spot_price[candles.symbol] = candles.close
            current_spot = candles.symbol
        elif candles.symbol in self.quote_futures_family:
            self.futures_price[candles.symbol] = candles.close
            for spot_symbol in self.quote_spot_family:
                for futures in self.symbol_mapping[spot_symbol]:
                    if candles.symbol == futures:
                        current_spot = spot_symbol
        if current_spot and current_spot in self.spot_price:
            print(f"{current_spot}: {self.spot_price[current_spot]}")
            for futures in self.symbol_mapping[current_spot]:
                if futures in self.futures_price:
                    print(f"{futures}: {self.futures_price[futures]}, futures/spot percentage: {(self.futures_price[futures]/self.spot_price[current_spot]-1)*100}%")

def main():
# async def main():
    binance_info = Binance.info()
    binance_futures_info = BinanceFutures.info()
    # binance_inverse_info = BinanceDelivery.info()

    binance_price_compare = BinancePriceCompare("usdt", ["btc", "eth", "sol", "bnb", "ada", "dot", "matic"])
    # if you want all assets should make a subclass of Binance.
    # binance socket accept only under 200 markets at once.
    # binance_price_compare = BinancePriceCompare("usdt")

    binance = Binance(max_depth=3, symbols=binance_price_compare.spot_symbols,
                      channels=[CANDLES],
                      callbacks={CANDLES: binance_price_compare.candles})
    binance_futures = BinanceFutures(max_depth=3, symbols=binance_price_compare.futures_symbols,
                                     channels=[CANDLES],
                                     callbacks={CANDLES: binance_price_compare.candles})

    f = FeedHandler()
    f.add_feed(binance)
    f.add_feed(binance_futures)
    f.run()

if __name__ == '__main__':
    main()
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(main())
