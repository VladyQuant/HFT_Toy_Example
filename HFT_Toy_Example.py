#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue May 22 18:48:43 2018

@author: Vladimir Tsygankov
"""

from multiprocessing import Process, Manager
import time
import random
from copy import deepcopy

class Exchange:
    """
    A Model of exchange matching engine. Generates order book and fills orders for a stock X.
    
    **Parameters**\n
    order_pipe:
        list of lists, contains clients unfilled orders.
    order_book:
        dictionary of lists, contains bids and asks for the stock X.
    order_filling:
        list of lists, contains info about filled clients orders.
    RUN_TIME:
        int, running time of the exchange in seconds.
    AVERAGE_PRICE, PRICE_SIGMA:
        ints, mean and standard deviation of gaussian distribution, which has the highest bid.
    GENERATION_RANGE:
        int, the exchange generates quotes every micro_delay microseconds, 
        where micro_delay is uniformly distributed in range(1,GENERATION_RANGE).
    
    """
    def __init__(self,order_pipe,order_book,order_filling):
        self.RUN_TIME = 30
        self.AVERAGE_PRICE = 25.
        self.PRICE_SIGMA = 0.5
        self.GENERATION_RANGE = 1000
        
        self.order_pipe = order_pipe
        self.order_book = order_book
        self.order_filling = order_filling
    
    def _generate_order_book(self):
        while True:
            #Set a random micro_delay for quotes generation.
            micro_delay = random.randint(1,self.GENERATION_RANGE) * 1e-6
            time.sleep(micro_delay)

            highest_bid = round(random.gauss(self.AVERAGE_PRICE,self.PRICE_SIGMA),2)
            volumes = random.sample(range(1,1000),6)
            lock.acquire()
            self.order_book[1] = [            #1 stands for Asks.
                [0, highest_bid + 0.05, volumes[2]], #lowest Ask.
                [1, highest_bid + 0.10, volumes[1]],
                [2,highest_bid + 0.15, volumes[0]]
            ]
            self.order_book[0] = [           #0 stands for Bids.
                [0, highest_bid, volumes[3]],#highest Bid. 
                [1, highest_bid - 0.05, volumes[4]],
                [2, highest_bid - 0.10, volumes[5]]
            ]
            lock.release()
    
    def _fill_order(self):  
        while True:
            time.sleep(1e-6)
            lock.acquire()
            if len(self.order_pipe) > 0:
                while len(self.order_pipe) > 0: #Fill all outstanding orders.
                    order = self.order_pipe.pop()
                    order_id, order_type, price, volume = order[0], order[1], order[2], order[3]
                    #Find match.
                    is_match = False
                    for match_candidate in self.order_book[not order_type]:
                        if match_candidate[1] == price: #match found.
                            match = match_candidate
                            is_match = True

                    
                    order_filling = [0,0,0,0,0]
                    if is_match: #Fill order.
                        if volume <= match[2]: #filled fully.
                            order_filling[0] = order_id
                            order_filling[1] = 0 #0 stands for fully filled.
                            order_filling[2] = order_type 
                            order_filling[3] = price
                            order_filling[4] = volume
                        else: #filled partially.
                            order_filling[0] = order_id
                            order_filling[1] = 1 #1 stands for partially filled.
                            order_filling[2] = order_type 
                            order_filling[3] = price
                            order_filling[4] = match[2] #Vollume filled.
                        #Update Order Book.
                        if volume < match[2]:
                            self.order_book[not order_type][match[0]][2] -= volume
                        else: 
                            del(self.order_book[not order_type][match[0]]) 
                    else: #No match
                        order_filling[0] = order_id
                        order_filling[1] = 2 #2 stands for REJECTED.
                    self.order_filling.append(order_filling)
            lock.release()
        
        
    def run(self):
        subprocesses = [0,0]
        subprocesses[0] = Process(target = self._generate_order_book)
        subprocesses[1] = Process(target = self._fill_order)
        
        for subprocess in subprocesses:
            subprocess.start()
        for subprocess in subprocesses:
            subprocess.join(self.RUN_TIME) #We force each subrpocess operate only the defined time.
            
        #Kill some still alive processes
        for subprocess in subprocesses:
            if subprocess.is_alive():
                # Terminate
                subprocess.terminate()
                subprocess.join()


class Trader:
    """
    A Model of HFT trader doing arbitrage on multiple exchanges.
    
    **Parameters**\n
    order_pipes:
        list of order_pipe to each exchange.
    order_book:
        list of order_book to each exchange.
    order_fillings:
        list of order_filling for each exchange.
    capital:
        int, capital.
    n_contracts:
        int, # of contracts in the stock X.
    comission:
        float, comission size in decimals.
    sent_requests:
        dictionary of lists of sent request IDs to each exchange.
    filled_requests:
        dictionary of lists of filled request IDs at each exchange.
    """
    def __init__(self,order_pipes,order_books,order_fillings,capital,n_contracts,
                 sent_requests,filled_requests):
        self.RUN_TIME = 30
        self.capital = capital
        self.n_contracts = n_contracts         #Number of shares divided by 100, i.e. number of contracts. It is negative for short positions.
        self.comission = 0.0003
        
        self.order_pipes = order_pipes
        self.order_books = order_books
        self.order_fillings = order_fillings
        self.n_exchanges = len(order_pipes)  #Number of exchanges
        self.exchanges_at_start = True #Indicator if the exchanges only start operation. Thus it needs to wait bids and asks.
        
        self.sent_requests, self.filled_requests = sent_requests,filled_requests
        

    
    def _trade(self):
        order_id = 0 #Set counter of orders to be sent. It also serves as order ID.
        while True:
            time.sleep(1e-6)
            
            
            #Check orders execution.
            lock.acquire()
            for exch_id in range(self.n_exchanges):
                while len(self.order_fillings[exch_id]) > 0:
                    order_filling = self.order_fillings[exch_id].pop()
                    if (order_filling[1] == 0) or (order_filling[1] == 1): #filled fully or partially.
                        coeff = 1 if order_filling[2]==0 else -1
                        order_price = order_filling[3]*order_filling[4]*100 #order_filling[3] = price, order_filling[4] = volume
                        self.capital.value -= (coeff+self.comission)*order_price 
                        self.n_contracts.value += coeff*order_filling[4]
                        self.filled_requests[exch_id] += [order_filling[0]]  #order_filling[0] = order_id                    
            lock.release()
            
            #If some positions were not closed, i.e. orders not fully filled.
            #We simply close them immediately on the 1st exchange, with exch_id = 0.
            #For simplicity, we just touch the upper line of order books, i.e. lowest asks or highest bids.
            if self.n_contracts.value != 0:
                lock.acquire()
                exch_id = 0 #Select the first exchange
                bid_or_ask = 1 if self.n_contracts.value > 0 else 0 #We will sell if we have positive number of contracts and vice versa.
                coeff = 1 if self.n_contracts.value > 0 else -1
                exch_id = 0
                vol0 = self.order_books[exch_id][bid_or_ask][0][2] #We just touch the upper level of order books.
                price0 = self.order_books[exch_id][bid_or_ask][0][1]
                vol_liquidate = min(abs(self.n_contracts.value),vol0) #Liquidate only available volume on the top of book.
                #Form order.
                exch_order = [order_id,bid_or_ask,price0,vol_liquidate]
                #Send order.
                self.order_pipes[exch_id].append(exch_order)
                #Add order IDs to list.
                self.sent_requests[exch_id] += [order_id]
                order_id += 1
                self.n_contracts.value -= coeff*vol_liquidate
                #Wait untill the exchange will execute.
                lock.release()
                time.sleep(1e-6)
            
            lock.acquire()
            #Wait untill all exchanges have generated quotes.
            if self.exchanges_at_start:
                for exch_id in range(self.n_exchanges):
                    while True:
                        if len(self.order_books[exch_id][0])==0:
                            lock.release()
                            time.sleep(5e-6)
                            lock.acquire()
                        else:
                            self.exchanges_at_start = False
                            break 
            
            #Look for arbitrage opportunities.
            is_arbitrage_opportunity = False #indicator of arbitrage opportunities.
            for exch_id in range(self.n_exchanges-1):
                highest_bid = self.order_books[exch_id][0][0][1]
                lowest_ask  = self.order_books[exch_id][1][0][1]
                for other_exch_id in range(exch_id + 1, self.n_exchanges):
                    other_highest_bid = self.order_books[other_exch_id][0][0][1]
                    other_lowest_ask  = self.order_books[other_exch_id][1][0][1]
                    #Check for arbitrage.
                    if highest_bid > other_lowest_ask:
                        is_arbitrage_opportunity = True
                        break
                    elif lowest_ask < other_highest_bid:
                        is_arbitrage_opportunity = True
                        #Permute exchanges, since the situation is simmetrical to the above.
                        exch_id, other_exch_id = other_exch_id, exch_id
                        highest_bid, other_lowest_ask = other_highest_bid, lowest_ask
                        break
            
            #Do arbitrage.
            if is_arbitrage_opportunity:
                exch_bids = deepcopy(self.order_books[exch_id][0])
                other_exch_asks = deepcopy(self.order_books[other_exch_id][1])
                while highest_bid > other_lowest_ask: #Iteratively grab all order books 
                    #discrepancies between the two order books.
                    highest_bid_vol = exch_bids[0][2]
                    lowest_ask_vol  = other_exch_asks[0][2]
                    #Start order forming.
                    order_volume = min(highest_bid_vol,lowest_ask_vol) #Grab available.
                    
                    exch_order = [order_id,1,highest_bid,order_volume]
                    other_exch_order = [order_id+1,0,other_lowest_ask,order_volume]
                    #Send orders
                    self.order_pipes[exch_id].append(exch_order)
                    self.order_pipes[other_exch_id].append(other_exch_order)
                    #Add their IDs to list.
                    self.sent_requests[exch_id] += [order_id]
                    self.sent_requests[other_exch_id] += [order_id+1]
                    order_id += 2
                    #remove from the copy of order books.
                    if lowest_ask_vol == highest_bid_vol:
                        exch_bids = exch_bids[1:]
                        other_exch_asks = other_exch_asks[1:]
                    elif highest_bid_vol > lowest_ask_vol:
                        other_exch_asks = other_exch_asks[1:]
                        exch_bids[0][2] -= lowest_ask_vol
                    else:
                        exch_bids = exch_bids[1:]
                        other_exch_asks[0][2] -= highest_bid_vol
                        
                        
                    #Set new highest_bid and other_lowest_ask.
                    if (len(exch_bids) != 0) and (len(other_exch_asks) != 0):
                        highest_bid = exch_bids[0][1]
                        other_lowest_ask = other_exch_asks[0][1]
                    else:
                        break
            
            lock.release()
        
    def run(self):
        subprocess = Process(target=self._trade)
        subprocess.start()
        subprocess.join(self.RUN_TIME)
        
        if subprocess.is_alive():
            subprocess.terminate()
            subprocess.join()

if __name__ == '__main__':
    random.seed(1)
    CAPITAL = 20000000 #Start capital.
    
    #Initialize variables, which will be shared during or stored after the 
    #parallel processing.
    manager = Manager()
    #0 at endinngs stand for 1st exchange, 1 for the second.
    order_book0, order_book1 = manager.dict(), manager.dict() #Order books.
    order_pipe0, order_pipe1 = manager.list(), manager.list() #Order pipes from clients.
    order_filling0, order_filling1 = manager.list(), manager.list() #Order execution reports from exchanges.
    
    order_book0[0], order_book1[0] = [], []    #0 stands for Bids.
    order_book0[1], order_book1[1] = [], []    #1 stands for Asks.
    
    capital = manager.Value('d',CAPITAL)
    n_contracts = manager.Value('i',0) #Number of contracts in the stock X.
    sent_requests, filled_requests = manager.dict(), manager.dict() #IDs of sent and filled request from each exchange.
    for exch_id in range(2):
        sent_requests[exch_id], filled_requests[exch_id] = [], [] 
    
    lock = manager.Lock()
    
    
    #Set up 2 exchanges.
    exchange0 = Exchange(order_pipe=order_pipe0,order_book=order_book0,
                         order_filling=order_filling0)
    exchange1 = Exchange(order_pipe=order_pipe1,order_book=order_book1,
                         order_filling=order_filling1)
    #Set up trader.
    trader = Trader(order_pipes=[order_pipe0, order_pipe1],
                    order_books=[order_book0, order_book1],
                    order_fillings=[order_filling0, order_filling1],
                    capital=capital, n_contracts = n_contracts,
                    sent_requests=sent_requests,
                    filled_requests=filled_requests)
    
    processes = [Process(target = exchange0.run),
                Process(target = exchange1.run),
                Process(target = trader.run)]
    for process in processes:
        process.start()
    
    for process in processes:
        process.join()
    
    n_trades_executed = len(trader.filled_requests[0]) + len(trader.filled_requests[1])
    print "Start capital: ", CAPITAL
    print "Final capital: ", trader.capital.value
    print "Run time: {} seconds." .format(trader.RUN_TIME)
    print "# of trades executed: ", n_trades_executed