# bitFlyerScalpingBot.py
# -*- coding: utf-8 -*-

from pubnub.callbacks import SubscribeCallback
from pubnub.enums import PNStatusCategory
from pubnub.pnconfiguration import PNConfiguration
from pubnub.pubnub_tornado import PubNubTornado
from pubnub.pnconfiguration import PNReconnectionPolicy
from tornado import gen
import pandas as pd
from datetime import datetime, timezone, timedelta
import pybitflyer
import json


# bitFlyer API 設定
public_api = pybitflyer.API()
bitFlyer_keys = json.load(open('bitFlyer_keys.json', 'r'))
api = pybitflyer.API(api_key = bitFlyer_keys["key"], api_secret = bitFlyer_keys["secret"])

# 約定履歴保管df
df_all = pd.DataFrame(index=['datetime'],
                    columns=['id', 
                            'side', 
                            'price', 
                            'size', 
                            'exec_date', 
                            'buy_child_order_acceptance_id', 
                            'sell_child_order_acceptance_id'])

# 初期ポジションを取得
bf_positions = pd.DataFrame(api.getpositions(product_code = "FX_BTC_JPY"))
local_pos = 'NonePos'
if not(bf_positions.empty):
    bf_positions = bf_positions['side'].values.flatten()
    if bf_positions == 'BUY': local_pos = 'BuyPos'
    elif bf_positions == 'SELL': local_pos = 'SellPos'



# 20秒間の約定履歴を保管して売買volumeを返す
def store_executions(channel, message):
    for i in message:
        df_new = pd.DataFrame(message)
        df_new['exec_date'] = pd.to_datetime(df_new['exec_date'])

    global df_all
    df_all = df_all.append(df_new)
    df_all.index = df_all['exec_date']

    date_now = df_all.index[len(df_all)-1]
    df_lim = df_all.ix[df_all.index >= (date_now - timedelta(seconds=20))]

    buy_vol = df_lim[df_lim.apply(lambda x: x['side'], axis=1) == "BUY"]['size'].sum(axis=0)
    sell_vol = df_lim[df_lim.apply(lambda x: x['side'], axis=1) == "SELL"]['size'].sum(axis=0)
    ex_price = int(df_lim.ix[[len(df_lim)-1],['price']].values.flatten())

    return df_lim, buy_vol, sell_vol, ex_price


# pubnub message受信毎に行う処理
def task(channel, message):
    df_lim, buy_vol, sell_vol, ex_price = store_executions(channel, message)

    global local_pos
    
    # 注文パラメーター
    order_margin = 10
    order_size = 0.001
    
    # 買い優勢
    if buy_vol > sell_vol:
        # ショート決済注文
        if local_pos == 'SellPos':
            # 最終オーダーのポジション、価格を取得
            bf_positions = pd.DataFrame(api.getpositions(product_code = "FX_BTC_JPY"))
            if not(bf_positions.empty):
                last_position = bf_positions['side'].values.flatten()
                last_price = int(bf_positions['price'].values.flatten())
                if last_position == 'SELL':
                    # 現在の中間価格を取得
                    mid_price = public_api.board(product_code = "FX_BTC_JPY")["mid_price"]
                    print("[Close Short position]", "profit:", -(mid_price - last_price)*order_size)
                    callback = api.sendchildorder(product_code = "FX_BTC_JPY", child_order_type = "MARKET", side = "BUY", size = order_size)
                    print(callback)
                    if not(callback.get('status')): local_pos = 'NonePos'
        # ロングエントリー
        if local_pos == 'NonePos' and (buy_vol - sell_vol) > order_margin:
            print("[Long Entry]")
            callback = api.sendchildorder(product_code = "FX_BTC_JPY", child_order_type = "MARKET", side = "BUY", size = order_size)
            print(callback)
            if not(callback.get('status')): local_pos = 'BuyPos'
    
    # 売り優勢
    if sell_vol > buy_vol:
        # ロング決済注文
        if local_pos == 'BuyPos':
            # 最終オーダーのポジション、価格を取得
            bf_positions = pd.DataFrame(api.getpositions(product_code = "FX_BTC_JPY"))
            if not(bf_positions.empty):
                last_position = bf_positions['side'].values.flatten()
                last_price = int(bf_positions['price'].values.flatten())
                if last_position == 'BUY':
                    # 現在の中間価格を取得
                    mid_price = public_api.board(product_code = "FX_BTC_JPY")["mid_price"]
                    print("[Close Long position]", "profit:", (mid_price - last_price)*order_size)
                    callback = api.sendchildorder(product_code = "FX_BTC_JPY", child_order_type = "MARKET", side = "SELL", size = order_size)
                    print(callback)
                    if not(callback.get('status')): local_pos = 'NonePos'
        # ショートエントリー
        if local_pos == 'NonePos' and (sell_vol - buy_vol) > order_margin:
            print("[Short Entry]")
            callback = api.sendchildorder(product_code = "FX_BTC_JPY", child_order_type = "MARKET",side = "SELL", size = order_size)
            print(callback)
            if not(callback.get('status')): local_pos = 'SellPos'
    
    print(df_lim.index[len(df_lim)-1].strftime('%H:%M:%S'),
          "BUY_VOL", format(buy_vol, '.2f'),
          "SELL_VOL", format(sell_vol, '.2f'),
          "price", ex_price,
          "pos", local_pos)



# pubnub 受信処理
config = PNConfiguration()
config.subscribe_key = 'sub-c-52a9ab50-291b-11e5-baaa-0619f8945a4f'
config.reconnect_policy = PNReconnectionPolicy.LINEAR
pubnub = PubNubTornado(config)
@gen.coroutine #非同期処理
def main(channels):
    class BitflyerSubscriberCallback(SubscribeCallback):
        def presence(self, pubnub, presence):
            pass  # handle incoming presence data

        def status(self, pubnub, status):
            if status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
                pass  # This event happens when radio / connectivity is lost

            elif status.category == PNStatusCategory.PNConnectedCategory:
                # Connect event. You can do stuff like publish, and know you'll get it.
                # Or just use the connected event to confirm you are subscribed for
                # UI / internal notifications, etc
                pass
            elif status.category == PNStatusCategory.PNReconnectedCategory:
                pass
                # Happens as part of our regular operation. This event happens when
                # radio / connectivity is lost, then regained.
            elif status.category == PNStatusCategory.PNDecryptionErrorCategory:
                pass
                # Handle message decryption error. Probably client configured to
                # encrypt messages and on live data feed it received plain text.

        def message(self, pubnub, message):
            # Handle new message stored in message.message
            task(message.channel, message.message)

    listener = BitflyerSubscriberCallback()
    pubnub.add_listener(listener)
    pubnub.subscribe().channels(channels).execute()


if __name__ == '__main__':
    main(['lightning_executions_FX_BTC_JPY'])
    pubnub.start()
