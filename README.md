# HFT_Toy_Example
A toy model of high-frequency trading (HFT) arbitrage in Python

Models of an exchange matching engine and an HFT arbitrageur or trader. Run on parallel processes. The trader grabs opportunities on 2 exchanges. 

1. Program is written for Python 2.7, and >= 3.
2. 'multiprocessing' library is used, which is standard for most python's distributions.
3. Run time of the exchange matching engine and the trader can be set up manually in the corresponding classes. The program        requires 5 free CPU kernels for the exact run time operation. Two kernels for each exchange (1 for quotes generation and 1 for order filling) and 1 for the trader. Otherwise, it will lag. In the example, the set up run time is 30 seconds, but it operates during 1 minute on a 2 kernel CPU.
4. A user should just lunch the program. After the exchanges and the trader have finished the trade, it will output: initial and final capitals, and number of filled orders.
