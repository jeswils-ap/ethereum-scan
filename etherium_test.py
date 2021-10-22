'''
Created on 21 Oct 2021
@author: jessewilson
'''
import requests, json, logging, sys, os
import pandas as pd
from time import time
from datetime import datetime
from requests.exceptions import RequestException
from flask import Flask, request, render_template, url_for

logging.basicConfig(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

__api_key__ = '1IDVMG3ZGGGGRQUUXX262BADNJ1EK8Z9N4'
__wei_to_eth__ = 10 ** 18
IMAGE_FOLDER = os.path.join(os.getcwd(), 'images')

app = Flask(__name__, template_folder='template')
app.config['IMAGES'] = IMAGE_FOLDER
@app.route("/", methods=['GET'])
def frontPage():
    return render_template('index.html')

@app.route("/crawl", methods=['POST'])
def crawl():
    address = request.form['address']
    start_block = request.form['block']
    
    end_block = get_current_block()
    normalTrans = get_normal_transactions(address, start_block, end_block)
    internalTrans = get_internal_transactions(address, start_block, end_block)
    
    normalTransDf = create_datafram(normalTrans)
    internalTransDf = create_datafram(internalTrans)

    mergedFrame = merge_frames([normalTransDf, internalTransDf])
    finalDf = get_token_details(mergedFrame)
    
    dfHtml = table_to_html(finalDf)
    #path = os.path.join(app.config['IMAGES'], 'ethValues.png')
    #print(path)
    
    return render_template('results.html', wallet=address, tableText=dfHtml)

def get_current_block():
    url = 'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={0}&closest=before&apikey={1}'.format(int(time()), __api_key__)
    try:
        logger.info("Fetching latest Etherium block no.")
        block = json.loads(requests.get(url).text)
    except RequestException as e:
        logger.error("Error getting latest block {0}".format(e))
    
    if 'result' in block:
        logger.info("Finished fetching latest Etherium block no.")    
        return block['result']
    else:
        logger.error("Block number not found in response.")
        sys.exit("Block number not found in response.")

def get_normal_transactions(address, start_block, end_block):
    url = 'https://api.etherscan.io/api?module=account&action=txlist&address={0}&startblock={1}&endblock={2}&page=1&offset=10&sort=asc&apikey={3}'.format(address, start_block, end_block, __api_key__)
    try:
        logger.info("Fetching transactions for address {0}, starting at block {1}".format(address, start_block))
        transactions = json.loads(requests.get(url).text)
    except RequestException as e:
        logger.error("Error fetching transactions {0}".format(e))
    
    if 'result' in transactions and len(transactions['result']) > 0:
        logger.info("Finished fetching latest Etherium transactions.")    
        return transactions['result']
    else:
        logger.error("Transaction list empty.")
        sys.exit("Transaction list empty.")

def get_internal_transactions(address, start_block, end_block):
    url = 'https://api.etherscan.io/api?module=account&action=txlistinternal&address={0}&startblock={1}&endblock={2}&page=1&offset=10&sort=asc&apikey={3}'.format(address, start_block, end_block, __api_key__)
    try:
        logger.info("Fetching internal transactions for address {0}, starting at block {1}".format(address, start_block))
        transactions = json.loads(requests.get(url).text)
    except RequestException as e:
        logger.error("Error fetching internal transactions {0}".format(e))
    
    if 'result' in transactions and len(transactions['result']) > 0:
        logger.info("Finished fetching latest Etherium transactions.")    
        return transactions['result']
    else:
        logger.error("Transaction list empty.")
        sys.exit("Transaction list empty.")   

def convert_timestamp(epoch):
    return datetime.fromtimestamp(epoch).strftime('%Y-%m-%d %H:%M:%S')

def create_datafram(transactions):
    logger.info("Loading transactions into dataframe.")
    transDf = pd.DataFrame(transactions)
    
    logger.debug("Setting block number as dataframe index.")
    transDf.set_index('blockNumber')
    
    logger.debug("Converting epoch time.")
    transDf['timeStamp'] = transDf['timeStamp'].apply(lambda x: convert_timestamp(int(x)))
    
    logger.debug("Converting value column from string to float")
    transDf = transDf.convert_dtypes()
    transDf['value'] = transDf['value'].astype('float64')
    
    logger.debug("Calculating token count from wallet value.")
    transDf['ethValue'] = transDf['value'].apply(lambda x: x / __wei_to_eth__)
    
    logger.debug("Calculating change in wallet value.")
    transDf['delta'] = transDf['ethValue'].diff().fillna(0)
    
    return transDf

def merge_frames(frames):
    dropFrames = ['hash','nonce','blockHash','transactionIndex','txreceipt_status', 'gas','gasPrice','isError','cumulativeGasUsed','gasUsed', 'type','traceId','errCode','confirmations','input']
    
    logger.info("Merging frames.")
    union = pd.concat(frames)
    
    logger.debug("Dropping unnecessary columns from frame.")
    union.drop(columns=dropFrames, inplace=True)
    
    logger.debug("Resetting index of new frame to block number")
    union.set_index('blockNumber', inplace=True)
    
    return union

def get_eth_price():
    url = "https://min-api.cryptocompare.com/data/price?fsym=BTC&tsyms=USD"
    
    try:
        logger.debug("Fetching ETH USD value")
        value = json.loads(requests.get(url).text)
    except RequestException as e:
        logger.error("Error fetching ETH USD value {0}".format(e))
    
    if 'USD' in value:
        return float(value['USD'])
    else:
        pass
        

def get_token_details(transDf):
    url = 'https://api.etherscan.io/api?module=token&action=tokeninfo&contractaddress={0}&apikey={1}'
    ethPrice = get_eth_price()
     
    tempDf = transDf.copy()
    tempDf['tokenValue'] = pd.Series([], dtype='float64')
    tempDf['walletValue'] = pd.Series([], dtype='float64')
    
    logger.info("Parsing dataframe for contract address")
    for i, contract in tempDf['contractAddress'].iteritems():
        url.format(contract, __api_key__)
        if contract != '':
            logger.info("Contract address found: {0}".format(contract))
            try:
                logger.info("Fetching token details for {0}".format(contract))
                details = json.loads(requests.get(url.format(contract, __api_key__)).text)
                if 'result' in details:
                    tokenDetails = details['result']
                    if 'tokenPriceUSD' in tokenDetails:
                        tokenPrice = tokenDetails['tokenPriceUSD']
                        tempDf.loc[i, 'tokenValue'] = tokenPrice
                        tempDf.loc[i, 'walletValue'] = tempDf.loc[i, 'ethValue'] * tempDf.loc[i, 'tokenValue']
                    else:
                        pass
                else:
                    pass
            except RequestException as e:
                logger.error("Error fetching token details {0}".format(e))
        else:
            logger.debug("Setting ETH token details")
            tempDf.loc[i, 'token'] = "ETH"
            tempDf.loc[i, 'tokenValue'] = ethPrice
            tempDf.loc[i, 'walletValue'] = tempDf.loc[i, 'ethValue'] * tempDf.loc[i, 'tokenValue']
            
    return tempDf

def plot_data(transactionDf):
    pd.options.plotting.backend = 'plotly'
    fig = transactionDf.plot(x='timeStamp' , y=['ethValue','delta'])
    fig.write_image(file='./images/ethValues.png', format='png')
    fig.show()

def table_to_html(transactions):
    logger.debug("Converting table to HTML")
    return transactions.to_html()

if __name__ == '__main__':
    app.run()