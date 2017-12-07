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


# bitFlyer API setting
public_api = pybitflyer.API()
bitFlyer_keys = json.load(open('bitFlyer_keys.json', 'r'))
api = pybitflyer.API(api_key=bitFlyer_keys['key'], api_secret=bitFlyer_keys['secret'])

# dataframe for executions
df_all = pd.DataFrame(index=['datetime'],
                    columns=['id', 
                            'side', 
                            'price', 
                            'size', 
                            'exec_date', 
                            'buy_child_order_acceptance_id', 
                            'sell_child_order_acceptance_id'])

# get initial bitFlyer positions
bf_positions = pd.DataFrame(api.getpositions(product_code='FX_BTC_JPY'))
local_pos = 'NONE'
local_pos_price = 0
if not(bf_positions.empty):
    local_pos = bf_positions.ix[[0], ['side']].values.flatten()
    local_pos_price = int(bf_positions.ix[[0], ['price']].values.flatten())
sum_profit = 0


# calc buy and sell volume from lightning_executions_FX_BTC_JPY message
def store_executions(channel, message, store_time_sec):
    for i in message:
        df_new = pd.DataFrame(message)
        df_new['exec_date'] = pd.to_datetime(df_new['exec_date']) + timedelta(hours=9)

    global df_all

    df_all = df_all.append(df_new)
    df_all.index = df_all['exec_date']

    date_now = df_all.index[len(df_all) - 1]
    df_all = df_all.ix[df_all.index >= (date_now - timedelta(seconds=store_time_sec))]

    buy_vol = df_all[df_all.apply(lambda x: x['side'], axis=1) == 'BUY']['size'].sum(axis=0)
    sell_vol = df_all[df_all.apply(lambda x: x['side'], axis=1) == 'SELL']['size'].sum(axis=0)
    ex_price = int(df_all.ix[[len(df_all) - 1], ['price']].values.flatten())

    return df_all, buy_vol, sell_vol, ex_price


# close buy or sell position
def close(side, order_size, ex_price):
    oposit_side = 'NONE'
    if side == 'BUY':
        oposit_side = 'SELL'
    elif side == 'SELL':
        oposit_side = 'BUY'

    bf_positions = pd.DataFrame(api.getpositions(product_code='FX_BTC_JPY'))
    if not(bf_positions.empty):
        bf_pos = bf_positions.ix[[0], ['side']].values.flatten()
        bf_pos_price = int(bf_positions.ix[[0], ['price']].values.flatten())
        if bf_pos == side:
            print('[' + side + ' Close]')
            callback = api.sendchildorder(product_code='FX_BTC_JPY', child_order_type='MARKET', side=oposit_side, size=order_size)
            print(callback)
            if not(callback.get('status')):
                ordered_profit = 0
                if side == 'BUY':
                    ordered_profit = (ex_price - bf_pos_price) * order_size
                elif side == 'SELL':
                    ordered_profit = -(ex_price - bf_pos_price) * order_size
                print('Order Complete!', 'ex_price:', ex_price, 'pos_price:', bf_pos_price, 'profit:', format(ordered_profit, '.2f'))
                return 'NONE', ordered_profit
    else:
        return side, 0


# entry buy or sell position
def entry(side, order_size):
    print('[' + side + ' Entry]')
    callback = api.sendchildorder(product_code='FX_BTC_JPY', child_order_type='MARKET', side=side, size=order_size)
    print(callback)
    if not(callback.get('status')):
        print('Order Complete!')
        return side
    else:
        return 'NONE'


def received_message_task(channel, message):
    global local_pos
    global local_pos_price
    global sum_profit

    # order parameter
    store_time_sec = 20
    order_size = 0.001
    volume_triger = 60

    df, buy_vol, sell_vol, ex_price = store_executions(channel, message, store_time_sec)

    # calc profit and profit_rate
    order_profit = 0
    if local_pos == 'BUY':
        order_profit = (ex_price - local_pos_price) * order_size
    elif local_pos == 'SELL':
        order_profit = -(ex_price - local_pos_price) * order_size
    order_profit_rate = order_profit / (ex_price * order_size)

    # buy or sell close
    if (local_pos == 'BUY') and (buy_vol < sell_vol):
        local_pos, ordered_profit = close('BUY', order_size, ex_price)
        sum_profit = sum_profit + ordered_profit
    elif (local_pos == 'SELL') and (buy_vol > sell_vol):
        local_pos, ordered_profit = close('SELL', order_size, ex_price)
        sum_profit = sum_profit + ordered_profit

    # buy or sell entry
    if (local_pos == 'NONE'):
        if ((buy_vol - sell_vol) > volume_triger):
            local_pos = entry('BUY', order_size)
            if local_pos == 'BUY':
                local_pos_price = ex_price
        elif (-(buy_vol - sell_vol) > volume_triger):
            local_pos = entry('SELL', order_size)
            if local_pos == 'SELL':
                local_pos_price = ex_price

    # summary
    print(df.index[len(df) - 1].strftime('%H:%M:%S'),
          'BU/SE',
          format(buy_vol, '.2f'),
          format(sell_vol, '.2f'),
          'PRICE',
          ex_price,
          local_pos,
          format(order_profit, '.2f'),
          format(order_profit_rate, '.4f'),
          'SMPF',
          format(sum_profit, '.2f'))



config = PNConfiguration()
config.subscribe_key = 'sub-c-52a9ab50-291b-11e5-baaa-0619f8945a4f'
config.reconnect_policy = PNReconnectionPolicy.LINEAR
pubnub = PubNubTornado(config)

# pubnub receive
@gen.coroutine
def main(channels):
    class BitflyerSubscriberCallback(SubscribeCallback):
        def presence(self, pubnub, presence):
            pass
        def status(self, pubnub, status):
            if status.category == PNStatusCategory.PNUnexpectedDisconnectCategory:
                pass
            elif status.category == PNStatusCategory.PNConnectedCategory:
                pass
            elif status.category == PNStatusCategory.PNReconnectedCategory:
                pass
            elif status.category == PNStatusCategory.PNDecryptionErrorCategory:
                pass
        def message(self, pubnub, message):
            try:
                received_message_task(message.channel, message.message)
            except:
                print('Could not do received_message_task.')

    listener = BitflyerSubscriberCallback()
    pubnub.add_listener(listener)
    pubnub.subscribe().channels(channels).execute()


if __name__ == '__main__':
    main(['lightning_executions_FX_BTC_JPY'],
         )
    pubnub.start()
