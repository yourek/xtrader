import json
import socket
import logging
import time
import ssl
from threading import Thread
from datetime import datetime, timezone
import pandas as pd
import ta
from stockstats import StockDataFrame

import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf

# set to true on debug environment only
DEBUG = True

#default connection properites
DEFAULT_XAPI_ADDRESS        = 'xapi.xtb.com'
DEFAULT_XAPI_PORT           = 5124
DEFUALT_XAPI_STREAMING_PORT = 5125

# wrapper name and version
WRAPPER_NAME    = 'python'
WRAPPER_VERSION = '2.5.0'

# API inter-command timeout (in ms)
API_SEND_TIMEOUT = 100

# max connection tries
API_MAX_CONN_TRIES = 3

# logger properties
logger = logging.getLogger("jsonSocket")
FORMAT = '[%(asctime)-15s][%(funcName)s:%(lineno)d] %(message)s'
logging.basicConfig(format=FORMAT)

if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.CRITICAL)


class TransactionSide(object):
    BUY = 0
    SELL = 1
    BUY_LIMIT = 2
    SELL_LIMIT = 3
    BUY_STOP = 4
    SELL_STOP = 5

class TransactionType(object):
    ORDER_OPEN = 0
    ORDER_CLOSE = 2
    ORDER_MODIFY = 3
    ORDER_DELETE = 4

class JsonSocket(object):
    def __init__(self, address, port, encrypt = False):
        self._ssl = encrypt
        if self._ssl != True:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket = ssl.wrap_socket(sock)
        self.conn = self.socket
        self._timeout = None
        self._address = address
        self._port = port
        self._decoder = json.JSONDecoder()
        self._receivedData = ''

    def connect(self):
        for i in range(API_MAX_CONN_TRIES):
            try:
                self.socket.connect( (self.address, self.port) )
            except socket.error as msg:
                logger.error("SockThread Error: %s" % msg)
                time.sleep(0.25);
                continue
            logger.info("Socket connected")
            return True
        return False

    def _sendObj(self, obj): # sending object to a socket - makes susbscriptions
        msg = json.dumps(obj)
        self._waitingSend(msg)

    def _waitingSend(self, msg):
        if self.socket:
            sent = 0
            msg = msg.encode('utf-8')
            while sent < len(msg):
                sent += self.conn.send(msg[sent:]) # particular place where we send request to a socket
                logger.info('Sent: ' + str(msg))
                time.sleep(API_SEND_TIMEOUT/1000)

    def _read(self, bytesSize=4096):
        if not self.socket:
            raise RuntimeError("socket connection broken")
        while True:
            char = self.conn.recv(bytesSize).decode()
            self._receivedData += char
            print(self._receivedData)
            try:
                (resp, size) = self._decoder.raw_decode(self._receivedData)
                if size == len(self._receivedData):
                    self._receivedData = ''
                    break
                elif size < len(self._receivedData):
                    self._receivedData = self._receivedData[size:].strip()
                    break
            except ValueError as e:
                continue
        logger.info('Received: ' + str(resp))
        return resp

    def _readObj(self):
        msg = self._read()
        return msg

    def close(self):
        logger.debug("Closing socket")
        self._closeSocket()
        if self.socket is not self.conn:
            logger.debug("Closing connection socket")
            self._closeConnection()

    def _closeSocket(self):
        self.socket.close()

    def _closeConnection(self):
        self.conn.close()

    def _get_timeout(self):
        return self._timeout

    def _set_timeout(self, timeout):
        self._timeout = timeout
        self.socket.settimeout(timeout)

    def _get_address(self):
        return self._address

    def _set_address(self, address):
        pass

    def _get_port(self):
        return self._port

    def _set_port(self, port):
        pass

    def _get_encrypt(self):
        return self._ssl

    def _set_encrypt(self, encrypt):
        pass

    timeout = property(_get_timeout, _set_timeout, doc='Get/set the socket timeout')
    address = property(_get_address, _set_address, doc='read only property socket address')
    port = property(_get_port, _set_port, doc='read only property socket port')
    encrypt = property(_get_encrypt, _set_encrypt, doc='read only property socket port')


class APIClient(JsonSocket):
    def __init__(self, address=DEFAULT_XAPI_ADDRESS, port=DEFAULT_XAPI_PORT, encrypt=True):
        super(APIClient, self).__init__(address, port, encrypt)
        if(not self.connect()):
            raise Exception("Cannot connect to " + address + ":" + str(port) + " after " + str(API_MAX_CONN_TRIES) + " retries")

    def execute(self, dictionary):
        self._sendObj(dictionary)
        return self._readObj()

    def disconnect(self):
        self.close()

    def commandExecute(self,commandName, arguments=None):
        return self.execute(baseCommand(commandName, arguments))

class APIStreamClient(JsonSocket):
    def __init__(self, address=DEFAULT_XAPI_ADDRESS, port=DEFUALT_XAPI_STREAMING_PORT, encrypt=True, ssId=None, candlesFun=None,
                 tickFun=None, tradeFun=None, balanceFun=None, tradeStatusFun=None, profitFun=None, newsFun=None):
        super(APIStreamClient, self).__init__(address, port, encrypt)
        self._ssId = ssId

        self._candlesFun = candlesFun
        self._tickFun = tickFun
        self._tradeFun = tradeFun
        self._balanceFun = balanceFun
        self._tradeStatusFun = tradeStatusFun
        self._profitFun = profitFun
        self._newsFun = newsFun

        self._test = []
        self._streamData = []
        self._finalDataset = []#pd.DataFrame(columns=['ctm', 'date','open','close','high','low','vol'])

        if(not self.connect()):
            raise Exception("Cannot connect to streaming on " + address + ":" + str(port) + " after " + str(API_MAX_CONN_TRIES) + " retries")

        self._running = True
        self._t = Thread(target=self._readStream, args=())
        self._t.setDaemon(True)
        self._t.start()

    def _readStream(self):
        print('Reading stream...')
        i = 1
        while (self._running):
            msg = self._readObj()
            self._streamData.append(msg)
            logger.info("Stream received: " + str(msg))
            if (msg["command"]=='tickPrices'):
                self._tickFun(msg)
            elif (msg["command"]=='trade'):
                self._tradeFun(msg)
            elif (msg["command"]=="balance"):
                self._balanceFun(msg)
            elif (msg["command"]=="tradeStatus"):
                self._tradeStatusFun(msg)
            elif (msg["command"]=="profit"):
                self._profitFun(msg)
            elif (msg["command"]=="news"):
                self._newsFun(msg)
            elif(msg["command"]=="getCandles" or msg["command"]=="candle"):
                period = 5
                # row_to_add = self._candlesFun(msg, i, 5)
                # print(row_to_add)
                data = msg['data']
                self._test = data
                
                open = 0
                high = 0
                low = 0
                vol = 0
                if i%period >= 0:
                    if i%period == 1:
                        open = data['open']
                        high = 0
                        low = 0
                        vol = 0
                        #columns = ['ctm', 'date','open','close','high','low','vol']
                        #rows = pd.DataFrame(columns=columns)
                    else:
                        if data['high'] > high:
                            high = data['high']
                        if data['low'] < low:
                            low = data['low']
                        vol += data['vol']

                    if i%period == 0:
                        close = data['close']
                        date = data['ctmString'] # dateParser(data['ctmString'], '%b %d, %Y, %I:%M:%S %p')
                        ctm = data['ctm']
                        finalObj = {
                            'ctm': ctm,
                            'date': date,
                            'open': open,
                            'close': close,
                            'high': high,
                            'low': low,
                            'vol': vol
                        }
                        #row_to_add = pd.DataFrame([finalObj])
                        self._finalDataset.append(finalObj)
                # print(row_to_add)
                # if row_to_add != None:
                #     self._finalDataset.append(row_to_add, ignore_index=True)
                i+=1

    def disconnect(self):
        self._running = False
        self._t.join()
        self.close()

    def execute(self, dictionary):
        self._sendObj(dictionary)

    def subscribePrice(self, symbol):
        self.execute(dict(command='getTickPrices', symbol=symbol, streamSessionId=self._ssId))

    def subscribePrices(self, symbols):
        for symbolX in symbols:
            self.subscribePrice(symbolX)

    def subscribeTrades(self):
        self.execute(dict(command='getTrades', streamSessionId=self._ssId))

    def subscribeBalance(self):
        self.execute(dict(command='getBalance', streamSessionId=self._ssId))

    def subscribeTradeStatus(self):
        self.execute(dict(command='getTradeStatus', streamSessionId=self._ssId))

    def subscribeProfits(self):
        self.execute(dict(command='getProfits', streamSessionId=self._ssId))

    def subscribeNews(self):
        self.execute(dict(command='getNews', streamSessionId=self._ssId))

    def subscribeGetCandles(self, symbol):
        self.execute(dict(command='getCandles', symbol=symbol, streamSessionId=self._ssId))

    def unsubscribeCandles(self, symbol):
        self.execute(dict(command='stopCandles', symbol=symbol, streamSessionId=self._ssId))

    def unsubscribePrice(self, symbol):
        self.execute(dict(command='stopTickPrices', symbol=symbol, streamSessionId=self._ssId))

    def unsubscribePrices(self, symbols):
        for symbolX in symbols:
            self.unsubscribePrice(symbolX)

    def unsubscribeTrades(self):
        self.execute(dict(command='stopTrades', streamSessionId=self._ssId))

    def unsubscribeBalance(self):
        self.execute(dict(command='stopBalance', streamSessionId=self._ssId))

    def unsubscribeTradeStatus(self):
        self.execute(dict(command='stopTradeStatus', streamSessionId=self._ssId))

    def unsubscribeProfits(self):
        self.execute(dict(command='stopProfits', streamSessionId=self._ssId))

    def unsubscribeNews(self):
        self.execute(dict(command='stopNews', streamSessionId=self._ssId))


# Command templates
def baseCommand(commandName, arguments=None):
    if arguments==None:
        arguments = dict()
    return dict([('command', commandName), ('arguments', arguments)])

def loginCommand(userId, password, appName=''):
    return baseCommand('login', dict(userId=userId, password=password, appName=appName))


def procCandles(msg, i, period):
    print("Candle: ",msg)
    data = msg['data']
    print(i)

    if i%period >= 0:
        if i%period == 1:
            open = data['open']
            high = 0
            low = 0
            vol = 0
            #columns = ['ctm', 'date','open','close','high','low','vol']
            #rows = pd.DataFrame(columns=columns)
        else:
            if data['high'] > high:
                high = data['high']
            if data['low'] < low:
                low = data['low']
            vol += data['vol']

        if i%period == 0:
            close = data['close']
            date = data['ctmString'] # dateParser(data['ctmString'], '%b %d, %Y, %I:%M:%S %p')
            ctm = data['ctm']
            finalObj = {
                'ctm': ctm,
                'date': date,
                'open': open,
                'close': close,
                'high': high,
                'low': low,
                'vol': vol
            }
            row_to_add = pd.DataFrame([finalObj])
            return row_to_add
    return None


# example function for processing ticks from Streaming socket
def procTickExample(msg):
    print("TICK: ", msg)

# example function for processing trades from Streaming socket
def procTradeExample(msg):
    print("TRADE: ", msg)

# example function for processing trades from Streaming socket
def procBalanceExample(msg):
    print("BALANCE: ", msg)

# example function for processing trades from Streaming socket
def procTradeStatusExample(msg):
    print("TRADE STATUS: ", msg)

# example function for processing trades from Streaming socket
def procProfitExample(msg):
    print("PROFIT: ", msg)

# example function for processing news from Streaming socket
def procNewsExample(msg):
    print("NEWS: ", msg)

def commandObjectWithCredentials(loginObj: dict, commandObj: dict):
    return loginObj.update(commandObj)


def parse_date_to_unix_time(date_string):
    # Convert the date string to a datetime object
    date = datetime.strptime(date_string, "%d/%m/%Y")

    # Get the UTC timestamp in seconds since January 1, 1970
    timestamp = date.replace(tzinfo=timezone.utc).timestamp()

    # Convert seconds to milliseconds
    unix_time = int(timestamp * 1000)

    return unix_time

def dateParser(date_string, date_format):
    date_obj = datetime.strptime(date_string, date_format)
    return date_obj.strftime("%Y-%m-%d")


def xTrading(ssid):
    sclient = APIStreamClient(ssId=ssid, candlesFun=procCandles, tickFun=procTickExample, tradeFun=procTradeExample, profitFun=procProfitExample, tradeStatusFun=procTradeStatusExample)
    sclient.execute(dict(command='getCandles', streamSessionId=ssid, symbol='US500'))


def initialRange(client, userId, password, symbol):
    rangeObj = {
        #"end": parse_date_to_unix_time("1/06/2023"),
        "period": 60,
        "start": parse_date_to_unix_time("1/01/2023"),
        "symbol": symbol,
        "ticks": 10000
    }
    rangeArg = {
        "userId":userId, "password":password,
        "info": rangeObj
    }

    rangeData = client.execute(baseCommand("getChartRangeRequest", rangeArg))

    df = pd.DataFrame(rangeData['returnData']['rateInfos'])

    df['date'] = df['ctmString']#.apply(lambda x: dateParser(x, '%b %d, %Y, %I:%M:%S %p'))
    # df['date']=pd.to_datetime(df['date'])
    # df.set_index('date', inplace=True)
    df['close'] = df['open']+df['close']
    df['low'] = df['open']+df['low']
    df['high'] = df['open']+df['high']
    df.head()

    supports = df[df.low == df.low.rolling(5, center=True).min()].low
    resistances = df[df.high == df.high.rolling(5, center=True).max()].high

    levels = pd.concat([supports, resistances])
    levels = levels[abs(levels.diff()) > 100]
    #mpf.plot(df, type='candle', hlines=levels.to_list(), style='charles')

    df['rsi'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi()
    df['ema_25'] = ta.trend.EMAIndicator(close=df['close'], window=25).ema_indicator()
    df['ema_50'] = ta.trend.EMAIndicator(close=df['close'], window=50).ema_indicator()

    return (df,)

def main():

    # enter your login credentials here
    userId = 14899731 # 1812611
    password = "@Spider90"

    loginObj = dict(userId=userId, password=password)

    # create & connect to RR socket
    client = APIClient()

    # connect to RR socket, login
    loginResponse = client.execute(loginCommand(userId=userId, password=password))
    logger.info(str(loginResponse))

    #test = client.execute(baseCommand("getAllSymbols",loginObj))
    #len(test)

    #filtered_data = [obj for obj in test['returnData'] if 'GOOGL' in obj['symbol']]
    rangeObj = {
        #"end": parse_date_to_unix_time("1/06/2023"),
        "period": 60,
        "start": parse_date_to_unix_time("1/01/2023"),
        "symbol": "US500",
        "ticks": 10000
    }

    rangeArg = {
        "userId":userId, "password":password,
        "info": rangeObj
    }

    test2 = client.execute(baseCommand("getChartRangeRequest", rangeArg))
    len(test2['returnData']['rateInfos'])


    df = pd.DataFrame(test2['returnData']['rateInfos'])


    df['date'] = df['ctmString']#.apply(lambda x: dateParser(x, '%b %d, %Y, %I:%M:%S %p'))
    # df['date']=pd.to_datetime(df['date'])
    # df.set_index('date', inplace=True)
    df['close'] = df['open']+df['close']
    df['low'] = df['open']+df['low']
    df['high'] = df['open']+df['high']
    df.head()

    supports = df[df.low == df.low.rolling(5, center=True).min()].low
    resistances = df[df.high == df.high.rolling(5, center=True).max()].high

    levels = pd.concat([supports, resistances])
    levels = levels[abs(levels.diff()) > 100]
    #mpf.plot(df, type='candle', hlines=levels.to_list(), style='charles')

    df['rsi'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi()
    df['ema_25'] = ta.trend.EMAIndicator(close=df['close'], window=25).ema_indicator()
    df['ema_50'] = ta.trend.EMAIndicator(close=df['close'], window=50).ema_indicator()


    fig, ax1 = plt.subplots()
    # Plotting the first line
    ax1.plot(df['close'], color='blue', label='Close')
    ax1.plot(df['ema_25'], color='green', label='EMA 25')
    ax1.plot(df['ema_50'], color='orange', label='EMA 50')

    # Adding a separate y-axis and plotting another line
    ax2 = ax1.twinx()
    ax2.plot(df['rsi'], color='red', label='RSI')

    # Set the labels and title
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Price')
    ax2.set_ylabel('RSI')

    # Display the legend
    ax1.legend(loc='upper left')
    ax2.legend(loc='upper right')

    plt.show()




    # df['rolling_min'] = df['close_value'].rolling(window=30).min()
    # df['rolling_max'] = df['close_value'].rolling(window=30).max()

    # # Calculate support and resistance levels
    # support_level = df['rolling_min'].nsmallest(n=100).iloc[-1]
    # resistance_level = df['rolling_max'].nlargest(n=10).iloc[-1]

    # stock = StockDataFrame.retype(df)

    # df['support_level'] = ta.trend.support(df['close'], window=30, method='sma')
    # df['resistance_level'] = ta.trend.resistance(df['close'], window=30, method='sma')
    # # Calculate support and resistance levels
    # stock['support_level'] = stock['close_value'].support()
    # stock['resistance_level'] = stock['close_value'].resistance()

    # check if user logged in correctly
    if(loginResponse['status'] == False):
        print('Login failed. Error code: {0}'.format(loginResponse['errorCode']))
        return

    # get ssId from login response
    ssid = loginResponse['streamSessionId']

    # second method of invoking commands
    #resp = client.commandExecute('getAllSymbols')

    # create & connect to Streaming socket with given ssID
    # and functions for processing ticks, trades, profit and tradeStatus
    sclient = APIStreamClient(ssId=ssid, candlesFun=procCandles, tickFun=procTickExample, tradeFun=procTradeExample, profitFun=procProfitExample, tradeStatusFun=procTradeStatusExample)

    sclient.subscribeGetCandles('GOOGL.US_4')
    sclient.unsubscribeCandles('GOOGL.US_4')
    # testStream = sclient.execute(dict(command='getTickPrices', symbol='GOOGL.US_4'))
    # procTickExample()

    # testStream = sclient.execute(dict(command="getChartRangeRequest",
    #                                   ))
    # sclient._receivedData
    # sclient._readStream()
    # # subscribe for trades
    # sclient.subscribeTrades()


    # def subscribeGetChargeRange(self, rangeObj):
    #     self.execute(dict(command="getChartRangeRequest", info=rangeObj, streamSessionId=self._ssId))
    # sclient.subscribeGetChargeRange(rangeObj)
    # subscribe for prices
    sclient.subscribePrices(['EURUSD', 'EURGBP', 'EURJPY'])
    sclient.unsubscribePrices(['EURUSD', 'EURGBP', 'EURJPY'])
    sclient.subscribeGetCandles('EURUSD')



    sclient.execute(dict(command='getNews', streamSessionId=ssid))
    sclient.execute(dict(command='stopNews', streamSessionId=ssid))
    sclient.execute(dict(command='getCandles', streamSessionId=ssid, symbol='US500'))
    sclient.execute(dict(command='stopCandles', streamSessionId=ssid, symbol='US500'))
    
    sclient._finalDataset
    sclient._streamData 
    sclient._test['ctm']
    pd.DataFrame([sclient._streamData[0]['data']])

    sclient.subscribeProfits()

    # this is an example, make it run for 5 seconds
    time.sleep(5)

    # gracefully close streaming socket
    sclient.disconnect()

    # gracefully close RR socket
    client.disconnect()


if __name__ == "__main__":
    main()
