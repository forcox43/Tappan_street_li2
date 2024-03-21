import requests
import pandas as pd
import time
from keys import api_key
from endpoints import base_url, case_ep, trader_ep, limits_ep, news_ep, assets_ep, securities_ep, tenders_ep, leases_ep, his_ep, order_book_ep, tas_ep, orders_ep, id_ep, bulk_cancel_ep
from trading_functions import get_case, get_trader_info, get_assets, get_securities, get_limits_tracker, get_securities_hist, get_orders, get_securities_book, post_order, get_orderss, get_time_and_sales, post_cancel_all_orders
from ignition import kick_start
kick_start() #tests the connection and status of the game as being active or stopped before proceeding.

directory = 'C:\\Users\\forre\\OneDrive\\Desktop\\RIT_Trading'

stock = 'GTA'

i = .5
limit_pull = 200
flip_rate = .01 #percent arbitrage installed in positions submitted to unravel each prior position created.
flip_qty = 2000
limit_threshold = 10000 #addresses game's limit stock holdings rule.
cnt_post_err = 0
cnt_post_err_LO = 0
cnt_post_err_LU = 0
cnt_post_err_2 = 0
error_threshold = 5
while i > 0:
    kick_start() #tests the connection and status of the game as being active or stopped before proceeding.
    print (get_securities(stock))
    #time.sleep(9)
    ####################################################################################
    # 0.) Get Tick From 
    ####################################################################################   
    hist = get_securities_hist(stock)
    print (hist)
    curr_tick = hist['tick'].iloc[0]
    print ('***CURRENT TICK IS: ' + str(curr_tick + 1))
    print ('Last Tick numbers: ')
    print (' OPEN |  HIGH |  LOW | CLOSE')
    op = hist['open'].iloc[0]
    hi = hist['high'].iloc[0]
    lo = hist['low'].iloc[0]
    cl = hist['close'].iloc[0]
    print ( str(op) + ' | ' + str(hi) + ' |  ' + str(lo) + ' | ' + str(cl))    

    ####################################################################################
    # 1.) Pulls the current Trader Book and finds the Bid Ask spread among the book.
    ####################################################################################    
    
    print (get_securities_book(stock, limit_pull))
    result = get_securities_book(stock, limit_pull)
    #result.to_csv(directory + '\\data\\result_' + str(i) + '.csv', index=False)
    result_bid = result[result["action"] == 'BUY'].reset_index(drop=True)
    result_ask = result[result["action"] == 'SELL'].reset_index(drop=True)
    print (result_bid)
    print (result_ask)

    BookTrader_ask = result_ask[["price","quantity","trader_id"]]
    BookTrader_ask = BookTrader_ask.rename(columns={"trader_id": "trader_id_ask","quantity":"quantity_a","price":"price_a"})
    BookTrader_ask = BookTrader_ask.sort_values(by='price_a', ascending = True)
    BookTrader_ask = BookTrader_ask.dropna()
    BookTrader_bid = result_bid[["trader_id","quantity","price"]]
    BookTrader_bid = BookTrader_bid.rename(columns={"price":"price_b","quantity":"quantity_b","trader_id": "trader_id_bid"})
    BookTrader_bid = BookTrader_bid.sort_values(by='price_b', ascending = True)
    BookTrader_bid = BookTrader_bid.dropna()
    BookTrader = pd.concat([BookTrader_bid,BookTrader_ask],axis=1, ignore_index = False)
    BookTrader['BA_Spread'] = BookTrader['price_b'] - BookTrader['price_a']
    BookTrader['BA_Spread_vol'] = BookTrader['price_b']*BookTrader['quantity_b'] - BookTrader['price_a']*BookTrader['quantity_a']
    
    #BookTrader.to_csv(directory + '\\data\\result_book_' + str(i) + '.csv', index=False)
    print (BookTrader)
    current_info = get_securities(stock)
    #print (current_info)
    current_info_pd = pd.json_normalize(current_info)
    current_info_pd = current_info_pd[['position','vwap','last','bid','bid_size','ask','ask_size','volume','unrealized','realized']]
    current_info_pd['BD_spd'] = current_info_pd['bid'] - current_info_pd['ask']
    #'position': -5000.0, 'vwap': 10.11, 'nlv': -50150.0, 'last': 10.01, 'bid': 10.01, 'bid_size': 300.0, 'ask': 10.03, 'ask_size': 1400.0, 'volume': 142700.0, 'unrealized': 400.0, 'realized': 1500.0
    print (current_info_pd)
    # ^^^^builds the same view as the Book Trader.
    
    ###
    ####################################################################################
    #2.) CHECKS for limit violation and addresses it with a buy or sell to correct. Uses Current Bid Ask spread from Securities/Portfolio table to trade on.
    ####################################################################################
    print (current_info_pd['position'])
    if current_info_pd['position'].item() > limit_threshold: 
        print ('WE NEED TO BE WITHIN LIMIT WERE OVER!!!!!!!')
        qty = current_info_pd['position'].item() - limit_threshold
        print(qty)
        print ('First check current orders..')
        ords = get_orderss()
        print (ords)
        if ords.empty == True:
            print ('Yes empty_ we need to sell')
            prc = current_info_pd['last'].item() - current_info_pd['BD_spd'].item()/2
            print(qty)
            if qty > 40000:
                qty = qty/10
            print (qty)
            print (prc)
            #ticker, order_type, quantity, side, price
            p = post_order(stock, 'LIMIT', qty, 'SELL', prc)
            print (p)
        else:
            ords_examine_under = ords[(ords['action'] == 'SELL') & (ords['tick'] > (curr_tick - 15))]
            qty_sell_in_action = ords_examine_under['quantity'].sum()
            print (qty_sell_in_action)
            print (type(qty_sell_in_action))
            print ('>=')
            print(qty)
            if qty_sell_in_action >= qty:
                print ('Order is already in..')#if ords['quantity'].sum() >= qty:
                prc = current_info_pd['last'].item() - current_info_pd['BD_spd'].item()/2
                #ticker, order_type, quantity, side, price
                p = post_order(stock, 'LIMIT', qty, 'SELL', prc)
                if ('INTERNAL_SERVER_ERROR' in str(p)) == True:
                    cnt_post_err_LO +=1
                    print ('Counting posting errors: ')
                    print (cnt_post_err_LO)
                    if cnt_post_err_LO > error_threshold:
                        post_cancel_all_orders(stock)
                        cnt_post_err_LO = 0

    elif current_info_pd['position'].item() < -limit_threshold:
    
        print ('WE NEED TO BE WITHIN LIMIT WERE UNDER!!!!!!!')
        qty = current_info_pd['position'].item() + limit_threshold
        print ('First check current orders..')
        ords = get_orderss()
        print (ords)
        if ords.empty == True:
            print ('Yes empty_we need to buy')
            prc = current_info_pd['last'].item() + current_info_pd['BD_spd'].item()/2
            print(qty)
            if qty < -40000:
                qty = qty/10
            print(qty)
            print (prc)
            #ticker, order_type, quantity, side, price
            p = post_order(stock, 'LIMIT', -1*qty, 'BUY', prc)
            print (p)
        else:
            ords_examine_under = ords[(ords['action'] == 'BUY') & (ords['tick'] > (curr_tick - 15))]
            qty_buy_in_action = ords_examine_under['quantity'].sum()
            
            print (qty_buy_in_action)
            print (type(qty_buy_in_action))
            print ('>=')
            print(qty)
            
            if qty_buy_in_action >= qty:
                print ('Order is already in..')#if ords['quantity'].sum() >= qty:
                prc = current_info_pd['last'].item() + current_info_pd['BD_spd'].item()/2
                print(qty)
                print (prc)
                #ticker, order_type, quantity, side, price
                p = post_order(stock, 'LIMIT', -1*qty, 'BUY', prc)
                print (p)
                if ('INTERNAL_SERVER_ERROR' in str(p)) == True:
                    cnt_post_err_LU +=1
                    print ('Counting posting errors: ')
                    print (cnt_post_err_LU)
                    if cnt_post_err_LU > error_threshold:
                        post_cancel_all_orders(stock)
                        cnt_post_err_LU = 0
    ###
    ####################################################################################
    #3.) Finds momentum based on the bid ask spread to trade on, to either iniate a trade on a BUY or a SELL, and submit a position to unravel the positioning.
    #################################################################################### 
    else:
        print ('CURRENT ORDERS SUBMITTED.......')
        ords = get_orderss()
        if ords.empty == True:
            print (ords)
            print ('LETS BUILD A TRADE!!!!')
            print ('Book Trader Bid Ask Spread: ')
            print (current_info_pd['BD_spd'].item())
            print ('Bid Ask Spread from the stock call function: ')
            curr_quote = get_securities(stock)
            curr_quote_df = pd.json_normalize(curr_quote)
            print (curr_quote_df['last'].item())
            print ('Current tick is: ' + str(curr_tick + 1))
            if (current_info_pd['BD_spd'].item() > -0.15 and current_info_pd['BD_spd'].item() < -0.03): #determines spread is narrow and liquid enough.
                print ('First test passed for building trade....')
                print (result_ask)
                print (result_ask.info)
                ask_VWAP = result_ask[result_ask['tick'] > (curr_tick - 20)]    #selects tick data no older than 10 ticks ago.
                ask_VWAP = ask_VWAP[ask_VWAP['vwap'] != 'None']  # needs to have VWAP data
                ask_VWAP = ask_VWAP[ask_VWAP['vwap'] != 'NaN']
                print ('PPPPPPPPPP')
                print (ask_VWAP)
                
                ask_VWAP_mean = ask_VWAP['vwap'].mean()
                print ('MEAN of VWAP:')
                print (ask_VWAP_mean) #calc the mean
                print ('Close of last tick price:')
                print (cl)
                print ('abs of VWAP mean and close price:')
                print (abs(ask_VWAP_mean - cl))
                #print (type (ask_VWAP_mean.item()))
                if abs(ask_VWAP_mean - cl) < .75:    #if the mean of VWAP ask/sell is within a margin of the last close price.
                    #ticker, order_type, quantity, side, price
                    p = post_order(stock, 'LIMIT', flip_qty, 'SELL', ask_VWAP_mean)
                    time.sleep (.5)
                    print (ask_VWAP_mean * flip_rate)
                    p = post_order(stock, 'LIMIT', flip_qty, 'BUY', ask_VWAP_mean * (1 - flip_rate))
                    if ('INTERNAL_SERVER_ERROR' in str(p)) == True:
                        cnt_post_err +=1
                        print ('Counting posting errors: ')
                        print (cnt_post_err)
                        if cnt_post_err > error_threshold:
                            post_cancel_all_orders(stock)
                            cnt_post_err = 0

        elif ords.empty != True:
            print ('HOW MANY ORDERS CURRENT? ')
            print (ords)
            if (current_info_pd['BD_spd'].item() > -0.15 and current_info_pd['BD_spd'].item() < -0.03): #determines spread is narrow and liquid enough.
                print ('First test passed for building trade....')
                print (result_ask)
                print (result_ask.info)
                ask_VWAP = result_ask[result_ask['tick'] > (curr_tick - 20)]    #selects tick data no older than 10 ticks ago.
                ask_VWAP = ask_VWAP[ask_VWAP['vwap'] != 'None']
                ask_VWAP = ask_VWAP[ask_VWAP['vwap'] != 'NaN']  # needs to have VWAP data
                print ('MMMMMMMMMMMMM')
                print (ask_VWAP)
                
                ask_VWAP_mean = ask_VWAP['vwap'].mean()
                print ('MEAN of VWAP:')
                print (ask_VWAP_mean) #calc the mean
                print ('Close of last tick price:')
                print (cl)
                print ('abs of VWAP mean and close price:')
                print (abs(ask_VWAP_mean - cl))
                #print (type (ask_VWAP_mean.item()))
                if abs(ask_VWAP_mean - cl) < .75:    #if the mean of VWAP ask/sell is within a margin of the last close price.
                    #ticker, order_type, quantity, side, price
                    post_order(stock, 'LIMIT', flip_qty, 'SELL', ask_VWAP_mean)
                    time.sleep (.5)
                    print (ask_VWAP_mean * flip_rate)
                    p = post_order(stock, 'LIMIT', flip_qty, 'BUY', ask_VWAP_mean * (1 - flip_rate))
                    if ('INTERNAL_SERVER_ERROR' in str(p)) == True:
                        cnt_post_err_2 +=1
                        print ('Counting posting errors_2: ')
                        print (cnt_post_err_2)
                        if cnt_post_err_2 > error_threshold:
                            post_cancel_all_orders(stock)
                            cnt_post_err_2 = 0
    
    pacer = .5
    time.sleep(pacer)
    print (i)
    print ('i')
    i+= pacer

print( 'COMPLETE')











"""
get_case() gets the current simulation Case name period number,
tick_per_period, total periods, status and enforcement of trading limits JSON object

returns a variable called case that is a JSON string
"""
"""
def get_case():
    with requests.Session() as sess:
        sess.headers.update(api_key)
        response = sess.get(base_url + case_ep)
        if response.ok:
            case = response.json()
    return case

"""
"""
get_assets() 
"""
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
"""
post_order()
"""
"""
def post_order(ticker, order_type, quantity, side, price):
    payload = {'ticker': ticker, 'type': order_type, 'quantity': quantity, 'action': side, 'price': price}
    with requests.Session() as sess:
        sess.headers.update(api_key)
        response = sess.post(base_url + orders_ep, params = payload)
        print(response)
        if response.ok:
            order_post_details = response.json()
            print(order_post_details)
    return order_post_details


"""


"""
print (get_case())

print (get_securities(stock))

print (get_securities_hist(stock))

print (get_securities_book(stock))
result = get_securities_book(stock)
result.to_csv('C:\\Users\\forre\\OneDrive\\Desktop\\RIT_Trading\\result.csv', index=False)
result_bid = result[result["action"] == 'BUY'].reset_index(drop=True)
result_ask = result[result["action"] == 'SELL'].reset_index(drop=True)
print (result_bid)
print (result_ask)

BookTrader_ask = result_ask[["trader_id","quantity","price"]]

BookTrader_bid = result_bid[["price","quantity","trader_id"]]
#BookTrader_bid['price'].rename
BookTrader = pd.concat([BookTrader_ask,BookTrader_bid],axis=1, ignore_index = False)
#print (BookTrader_ask, BookTrader_bid)
print (BookTrader)
# ^^^^builds the same view as the Book Trader.

print('^^^^^^^')
print('^^^^^^^')
print('^^^^^^^')
print('^^^^^^^')
print('^^^^^^^')
print (get_securities(stock))
"""

'''
base_url = "http://localhost:9999/v1/"
case_ep = "case"
trader_ep = "trader"
limits_ep = "limits"
news_ep = "news"
assets_ep = "assets"  BLANK
securities_ep = "securities"
tenders_ep = "tenders" BLANK
leases_ep = "leases" BLANK
his_ep = "/history"
order_book_ep = "/book"
tas_ep = "/tas"
orders_ep = "orders"
id_ep = "{id}"
bulk_cancel_ep = "commands/cancel"
'''

"""
def get_thing():
    with requests.Session() as sess:
        sess.headers.update(api_key)
        response = sess.get(base_url + securities_ep)
        if response.ok:
            thing = response.json()
    return thing


print (get_thing())
print(get_securities(stock))
print(';;;;;;;;;;;;')
print(get_securities_hist(stock))
get_securities_hist(stock).to_csv('C:\\Users\\forre\\OneDrive\\Desktop\\RIT_Trading\\result_hist.csv', index=False)



print (get_orderss())
print (get_assets(stock))
print(';;;;;;;;;;;;')
print (get_time_and_sales(stock,1))
print(';;;;;;;;;;;;')
print (get_time_and_sales(stock,0))
#print (post_order('HAR','MARKET',2000,'SELL',8.5))
#print (post_order('HAR','MARKET',2000,'BUY',8.5))
    
#print (post_order('HAR','LIMIT',2000,'SELL',8.5))
    
#if __name__ == "__main__":
#    main()
"""


"""

# Trace Grain RIT software program

# Import and load modules
import requests
import pandas
from securities import Securities
from orders import OrderBook
from keys import api_key
from endpoints import base_url, case_ep, trader_ep, limits_ep, news_ep, assets_ep, securities_ep, tenders_ep, leases_ep, his_ep, order_book_ep, tas_ep, orders_ep, id_ep, bulk_cancel_ep
from trading_functions import get_case, get_assets, get_securities, get_securities_hist, get_orders, get_securities_book, post_order


def main():
    print("Starting Program...")
    market_order_book = OrderBook(ticker = "HAR", api_key = api_key)

    x = market_order_book.get_order_book_quantity(side = "bids", quantity=1000, equality_side=">", col_subset_list="price")

    print(x)
if __name__ == "__main__":
    main()


"""