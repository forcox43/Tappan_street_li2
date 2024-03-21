
## This file houses all the trading
import requests
import pandas as pd
from keys import api_key
from endpoints import base_url, case_ep, assets_ep, securities_ep, his_ep, orders_ep, order_book_ep, tas_ep, bulk_cancel_ep

"""
PARAMETERS: None
RETURNS: case
DESCRIPTION: get_case() gets the current simulation Case name period number, 
tick_per_period, total periods, status and enforcement of trading limits JSON object
returns a variable called case that is a JSON string
"""
def get_case():
    with requests.Session() as sess:
        sess.headers.update(api_key)
        response = sess.get(base_url + case_ep)
        if response.ok:
            case = response.json()
    return case
    
"""
PARAMETERS: None
RETURNS:  {'trader_id': 'user2', 'first_name': '', 'last_name': '', 'nlv': 1399.99}
The NLV is Net Liquidation Value of all stocks. Combines Realized P&L with unrealized P&L 
excludes posted NLV in the GUI.
"""
def get_trader_info():
    with requests.Session() as sess:
        sess.headers.update(api_key)
        response = sess.get(base_url + trader_ep)
        if response.ok:
            trader_info = response.json()
    return trader_info

"""
PARAMETERS: None
RETURNS:  [{'name': 'LIMIT-STOCK', 'gross': 10000.0, 'net': -10000.0, 'gross_limit': 10000, 'net_limit': 10000, 'gross_fine': 0.0, 'net_fine': 0.0}]
Gross is the qty of stock, 
Net is the position of the total qty of stock
Gross limit is the qty of stock limit for the game in either direction.
"""
def get_limits_tracker():
    with requests.Session() as sess:
        sess.headers.update(api_key)
        response = sess.get(base_url + limits_ep)
        if response.ok:
            limits_tracker = response.json()
    return limits_tracker

"""
get_assets() 

"""
def get_assets(ticker):
    payload = {'ticker': ticker}
    with requests.Session() as sess:
        sess.headers.update(api_key)
        response = sess.get(base_url + assets_ep, params = payload)
        if response.ok:
            assets = response.json()
    return assets


"""
PARAMETERS: ticker

RETURNS:  
This shows the current portfolio of the trader. 
[
{'ticker': 'HAR', 'type': 'STOCK', 'size': 1, 'position': 0.0, 'vwap': 0.0, 'nlv': 0.0, 'last': 10.01, 'bid': 9.89, 'bid_size': 900.0, 
'ask': 10.01, 'ask_size': 200.0, 'volume': 9100.0, 'unrealized': 0.0, 'realized': 0.0, 'currency': '', 'total_volume': 9100.0, 
'limits': [{'name': 'LIMIT-STOCK', 'units': 1.0}], 
'interest_rate': 0.0, 'is_tradeable': True, 'is_shortable': True, 'start_period': 1, 'stop_period': 1, 
'description': 'HAR Common Shares', 'unit_multiplier': 1, 'display_unit': 'Shares', 'start_price': 10.0, 
'min_price': 2.0, 'max_price': 25.0, 'quoted_decimals': 2, 'trading_fee': 0.0, 'limit_order_rebate': 0.0, 
'min_trade_size': 0, 'max_trade_size': 50000, 'required_tickers': None, 'underlying_tickers': None, 'bond_coupon': 0.0, 
'interest_payments_per_period': 0, 'base_security': '', 'fixing_ticker': None, 'api_orders_per_second': 100, 
'execution_delay_ms': 0, 'interest_rate_ticker': None, 'otc_price_range': 0.0}
]
"""
def get_securities(ticker):
    payload = {'ticker': ticker}
    with requests.Session() as sess:
        sess.headers.update(api_key)
        response = sess.get(base_url + securities_ep, params = payload)
        if response.ok:
            securities = response.json()
    return securities

"""
get_securities_history()
RETURNS:  
This shows the current portfolio of the trader. 

"""
def get_securities_hist(ticker):
    payload = {'ticker': ticker}
    with requests.Session() as sess:
        sess.headers.update(api_key)
        response = sess.get(base_url + securities_ep + his_ep, params = payload)
        if response.ok:
            securities = response.json()
            securities_df = pd.json_normalize(securities)

    return securities_df


"""
get_securities_book()
Returns all active unfilled bids and asks as an appended dataframe for Bids and Asks.
BOOK TRADER
"""
def get_securities_book(ticker, num): #, side):
    payload = {'ticker': ticker, 'limit': num}
    with requests.Session() as sess:
        sess.headers.update(api_key)
        response = sess.get(base_url + securities_ep + order_book_ep, params = payload)
        if response.ok:
            sec_order_book = response.json()
            sec_order_book_ask_df = pd.json_normalize(sec_order_book['asks']) #SELLS
            print(sec_order_book_ask_df.shape)
            sec_order_book_bid_df = pd.json_normalize(sec_order_book['bids']) #BUYS
            print(sec_order_book_bid_df.shape)
            merged = sec_order_book_ask_df.append(sec_order_book_bid_df, ignore_index=True)
            merged = merged.sort_values(by='price', ascending = False)
            '''
            sec_order_book_ask_df = pd.json_normalize(sec_order_book['asks']) #SELLS
            sec_order_book_ask_df.columns = 'ask_' + sec_order_book_ask_df.columns.values
            sec_order_book_bid_df = pd.json_normalize(sec_order_book['bids']) #BUYS
            sec_order_book_bid_df.columns = 'bid_' + sec_order_book_bid_df.columns.values
            merged = pd.merge(sec_order_book_ask_df, sec_order_book_bid_df, left_on='ask_price', right_on='bid_price', how='outer') 
            merged = merged[[]]
            '''
    #return sec_order_book #_df 
    return merged

"""
get_time_and_sales()
Returns all active unfilled bids and asks as an appended dataframe for Bids and Asks.
BOOK TRADER
"""
def get_time_and_sales(ticker, binary): #, side):
    payload = {'ticker': ticker, 'after': binary}
    with requests.Session() as sess:
        sess.headers.update(api_key)
        response = sess.get(base_url + securities_ep + tas_ep, params = payload)
        if response.ok:
            sec_order_book = response.json()
            sec_order_book_ask_df = pd.json_normalize(sec_order_book) 
            
            '''
            sec_order_book_ask_df = pd.json_normalize(sec_order_book['asks']) #SELLS
            sec_order_book_ask_df.columns = 'ask_' + sec_order_book_ask_df.columns.values
            sec_order_book_bid_df = pd.json_normalize(sec_order_book['bids']) #BUYS
            sec_order_book_bid_df.columns = 'bid_' + sec_order_book_bid_df.columns.values
            merged = pd.merge(sec_order_book_ask_df, sec_order_book_bid_df, left_on='ask_price', right_on='bid_price', how='outer') 
            merged = merged[[]]
            '''
    #return sec_order_book #_df 
    return sec_order_book_ask_df
    
    
"""
get_orders() takes one parameter called order_type
order_type can take on the values of "OPEN", "TRANSACTED", "CANCELLED"
Returns all orders of the type specified 
response:
[{'order_id': 279, 'period': 1, 'tick': 182, 'trader_id': 'user2', 'ticker': 'HAR', 'quantity': 5000.0, 'price': 10.0, 'type': 'LIMIT', 'action': 'BUY', 'quantity_filled': 1400.0, 'vwap': 10.0, 'status': 'OPEN'}]
"""
def get_orders(order_type):
    payload = {'status': order_type}
    with requests.Session() as sess:
        sess.headers.update(api_key)
        response = sess.get(base_url + orders_ep, params = payload)
        if response.ok:
            orders = response.json()
    return orders

def get_orderss():
    with requests.Session() as sess:
        sess.headers.update(api_key)
        response = sess.get(base_url + orders_ep)
        if response.ok:
            orders_all = response.json()
            orderss = pd.json_normalize(orders_all)            
    return orderss

"""
SUBMITTING TRADING ORDERS WITH THIS FUNCTION
ticker should just equal 'stock'
order_type 'MARKET' or 'LIMIT'
quantity use float number 
side 'SELL' or 'BUY'
price use float number

post_order()

"""
def post_order(ticker, order_type, quantity, side, price):
    payload = {'ticker': ticker, 'type': order_type, 'quantity': quantity, 'action': side, 'price': price}
    with requests.Session() as sess:
        sess.headers.update(api_key)
        try:
            response = sess.post(base_url + orders_ep, params = payload)
            print(response)
            order_post_details = response.json()
            #print(order_post_details)       
        except requests.exceptions.HTTPError:
            order_post_details = requests.exceptions.HTTPError
            print (requests.exceptions.HTTPError) 
    return order_post_details


"""
CLEAR ALL CURRENT ORDERS
Through various parms

"""
def post_cancel_all_orders(ticker):
    ords = get_orderss()
    ords_ids = ords['order_id'].tolist()
    
    all_parm = 0 # This can be 0 for case parameter or 1 for all open orders.
    ticker_parm = ticker # Likely needs to be changed upon more than one ticker.
    ids_parm = ords_ids # Send in a list of all order ids that you want to cancel
    query_parm = ''   #GTA AND Price>10 AND Volume > 5000   |||  For example, Ticker='CL' AND Price>124.23 AND Volume<0
    
    payload = {'ticker': ticker_parm, 'all': all_parm, 'ids': ids_parm, 'query': query_parm}
    with requests.Session() as sess:
        sess.headers.update(api_key)
        try:
            print (payload)
            response = sess.post(base_url + bulk_cancel_ep, params = payload)
            print(response)
            post_cancel_all_orders_details = response.json()
            #print(post_cancel_all_orders_details)       
        except requests.exceptions.HTTPError:
            post_cancel_all_orders_details = requests.exceptions.HTTPError
            print (requests.exceptions.HTTPError) 
    return post_cancel_all_orders_details