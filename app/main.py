from flask import Flask, render_template, redirect, url_for, request, make_response, jsonify
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, SelectField
from wtforms.validators import DataRequired
from datetime import datetime
import requests
import json
import io
import csv
import time

app = Flask(__name__)
app.config['SECRET_KEY']='iloveperpprotocol'
Bootstrap(app)

MARKET_OPEN_TIMESTAMP = 1607914695

assets = {
    "0x922f28072babe6ea0c0c25ccd367fda0748a5ec7": "REN-USDC",
    "0x8d22f1a9dce724d8c1b4c688d75f17a2fe2d32df": "ETH-USDC",
    "0x0f346e19f01471c02485df1758cfd3d624e399b4": "BTC-USDC",
    "0xd41025350582674144102b74b8248550580bb869": "YFI-USDC",
    "0x6de775aabeeede8efdb1a257198d56a3ac18c2fd": "DOT-USDC",
    "0xb397389b61cbf3920d297b4ea1847996eb2ac8e8": "SNX-USDC",
    "0x80daf8abd5a6ba182033b6464e3e39a0155dcc10": "LINK-USDC",
    "0x16a7ecf2c27cb367df36d39e389e66b42000e0df": "AAVE-USDC",
    "0xf559668108ff57745d5e3077b0a7dd92ffc6300c": "SUSHI-USDC",
    "0x33fbaefb2dcc3b7e0b80afbb4377c2eb64af0a3a": "COMP-USDC",
    "0xfcae57db10356fcf76b6476b21ac14c504a45128": "PERP-USDC",
    "0xeac6cee594edd353351babc145c624849bb70b11": "UNI-USDC",
    "0xab08ff2c726f2f333802630ee19f4146385cc343": "CRV-USDC",
    "0xb48f7accc03a3c64114170291f352b37eea26c0b": "MKR-USDC",
    "0x7b479a0a816ca33f8eb5a3312d1705a34d2d4c82": "CREAM-USDC",
    "0x187c938543f2bde09fe39034fe3ff797a3d35ca0": "GRT-USDC",
    "0x26789518695b56e16f14008c35dc1b281bd5fc0e": "ALPHA-USDC",
    "0x838b322610bd99a449091d3bf3fba60d794909a9": "FTT-USDC"
}

class MainForm(FlaskForm):
    address = StringField('Ethereum address')
    submit_trades = SubmitField('Get Trades')
    amm = SelectField('Get Amm',choices=assets.values())
    submit_funding = SubmitField('Get Funding for Amm')

def get_all_position_changed_after_timestamp(trader, timestamp):
    query = """{
      positionChangedEvents(first: 1000, orderBy: timestamp, orderDirection: asc,where:{trader:"%s",timestamp_gt:"%s"}) {
        id
        trader
        amm
        margin
        positionNotional
        exchangedPositionSize
        fee
        positionSizeAfter
        realizedPnl
        unrealizedPnlAfter
        badDebt
        liquidationPenalty
        spotPrice
        fundingPayment
        timestamp
      }
    }
    """ % (trader, timestamp)
    url = 'https://api.thegraph.com/subgraphs/name/perpetual-protocol/perp-position-subgraph'
    r = requests.post(url, json={'query': query})
    json_data = json.loads(r.text)
    return json_data['data']['positionChangedEvents']

def get_all_trades(address):
    # Weiting Chen = boss
    has_data_remained = True
    positionChangedEvents_raw = []
    starting_timestamp = 1607914695
    while has_data_remained:
        latest_1k_positions = get_all_position_changed_after_timestamp(address,starting_timestamp)
        positionChangedEvents_raw.extend(latest_1k_positions)

        starting_timestamp = int(latest_1k_positions[-1]['timestamp'])
        if len(latest_1k_positions) < 1000:
            has_data_remained = False
    return positionChangedEvents_raw

def numparser(num):
    return int(num)/10**18

def dateparser(timestamp):
    return datetime.fromtimestamp(int(timestamp))

def build_trade_headers():
    return ['pair','size','price','PnL','transaction hash','timestamp']

def build_trade_row(trade):
    return [
    assets.get(str(trade['amm']),trade['amm']),
    numparser(trade['exchangedPositionSize']),
    numparser(trade['spotPrice']),
    numparser(trade['realizedPnl']),
    str(trade['id']),
    dateparser(trade['timestamp'])
    ]

def trades_to_csv(trades):
    si = io.StringIO()
    writer = csv.writer(si, quoting = csv.QUOTE_NONE)
    writer.writerow(build_trade_headers())
    for trade in trades:
        line = build_trade_row(trade)
        writer.writerow(line)
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=trades.csv"
    output.headers["Content-type"] = "text/csv"
    return output



def get_funding_changed_between_timestamps(amm,start_timestamp, end_timestamp):
    query = """{
      fundingRateUpdatedEvents(first: 1000, orderBy: timestamp, orderDirection: asc,where:{amm:"%s",timestamp_gt:"%s",timestamp_lt:"%s"}) {
        id
        rate
        underlyingPrice
        timestamp
      }
    }
    """ % (amm, start_timestamp, end_timestamp)
    url = 'https://api.thegraph.com/subgraphs/name/perpetual-protocol/perp-position-subgraph'
    r = requests.post(url, json={'query': query})
    json_data = json.loads(r.text)
    return json_data['data']['fundingRateUpdatedEvents']

def get_all_funding_changed_between_timestamps(amm,start_timestamp, end_timestamp):
    has_data_remained = True
    fundingRateEvents_raw = []
    starting_timestamp = start_timestamp
    while has_data_remained:
        latest_1k_funding = get_funding_changed_between_timestamps(amm,starting_timestamp,end_timestamp)
        time.sleep(0.5) #need to avoid overloading the graph
        fundingRateEvents_raw.extend(latest_1k_funding)

        if len(latest_1k_funding) > 0:
            starting_timestamp = int(latest_1k_funding[-1]['timestamp'])
            if len(latest_1k_funding) < 1000:
                has_data_remained = False
        else:
            has_data_remained = False #this might be an error so needs handling
    return fundingRateEvents_raw

def get_all_funding(address, amm):

    #Get all the trades the user made
    trades = get_all_trades(address)

    output = []

    #filter users trades to get the ones for this amm
    amm_trades = filter(lambda x: x['amm'] == amm.lower(),trades)
    lastFunding = 0
    size = 0
    for trade in amm_trades:
        if size != 0:
            funding = get_all_funding_changed_between_timestamps(trade['amm'],lastFunding, trade['timestamp'])
            for fund in funding:
                pos = {}
                pos['asset'] = assets.get(str(trade['amm']),trade['amm'])
                # pos['size'] = size
                pos['rate'] = numparser(fund['rate'])
                # pos['price'] = numparser(fund['underlyingPrice'])
                pos['timestamp'] = dateparser(fund['timestamp'])
                pos['payment'] = numparser(fund['underlyingPrice']) * size * numparser(fund['rate'])
                output.append(pos)

        lastFunding = trade['timestamp']
        size = numparser(trade['positionSizeAfter'])

    #This gets you funding up until last trade, but now need to get funding from that trade until now
    last_funding = get_all_funding_changed_between_timestamps(amm.lower(), lastFunding, int(time.time()))
    for fund in last_funding:
        pos = {}
        pos['asset'] = assets.get(str(trade['amm']),trade['amm'])
        # pos['size'] = size
        pos['rate'] = numparser(fund['rate'])
        # pos['price'] = numparser(fund['underlyingPrice'])
        pos['timestamp'] = dateparser(fund['timestamp'])
        pos['payment'] = numparser(fund['underlyingPrice']) * size * numparser(fund['rate'])
        output.append(pos)

    return output

def build_funding_headers():
    return ['pair','rate','payment','timestamp']

def build_funding_row(funding):
    return [
    assets.get(str(funding['asset']),funding['asset']),
    funding['rate'],
    funding['payment'],
    funding['timestamp']
    ]

def funding_to_csv(funding):
    si = io.StringIO()
    writer = csv.writer(si, quoting = csv.QUOTE_NONE)
    writer.writerow(build_funding_headers())
    for fund in funding:
        line = build_funding_row(fund)
        writer.writerow(line)
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=funding.csv"
    output.headers["Content-type"] = "text/csv"
    return output

@app.route('/', methods=['GET','POST'])
def hello():
    form = MainForm()
    if form.validate_on_submit():
        if form['submit_trades'].data:
            return trades_to_csv(get_all_trades(form.address.data))
        elif form['submit_funding'].data:
            for address, pair in assets.items():
                if pair == form.amm.data:
                    return funding_to_csv(get_all_funding(form.address.data, address))
    return render_template('index.html',form=form)

@app.route('/api/funding', methods=['GET'])
def return_funding():
    if 'address' in request.args:
        address = request.args['address']
    else:
        return "Error: Address not specified"
    if 'pair' in request.args:
        pair = request.args['pair']
    else:
        return "Error: Pair not specified"
    funding = get_all_funding(address, pair)
    return funding_to_csv(funding)

@app.route('/api/trades', methods=['GET'])
def return_trades():
    if 'address' in request.args:
        address = request.args['address']
    else:
        return "Error: Address not specified"
    trades = get_all_trades(address)
    return trades_to_csv(trades)
