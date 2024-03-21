import requests
import time
from keys import api_key
from endpoints import base_url, case_ep, trader_ep, limits_ep, news_ep, assets_ep, securities_ep, tenders_ep, leases_ep, his_ep, order_book_ep, tas_ep, orders_ep, id_ep, bulk_cancel_ep

def kick_start():    
    i = 1
    while i > 0:
        print ('***** Establishing initial connection...')
        with requests.Session() as sess:
            sess.headers.update(api_key)
            response = sess.get(base_url + case_ep)
            print ('***** Connection is ' + str(response.ok))
            if response.ok == False:
                print('...CONNECTION ISSUE... wait two seconds')
                time.sleep(2)
                print ('...')
                i = 1
                
            else:
                while response.ok == True:
                    case = response.json()
                    if case['status']=='ACTIVE':
                        print ('THIS IS ACTIVE')
                        i = 0
                        break
                    elif case['status']=='STOPPED':
                        print('THIS CASE IS STOPPED.. wait one second..')
                        time.sleep(1)
                        print ('...')
                        i = 1
                        break                