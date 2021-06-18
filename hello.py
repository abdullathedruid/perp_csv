from flask import Flask, request, make_response
from datetime import datetime
import requests
import json
import io
import csv

app = Flask(__name__)

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
    "0x26789518695b56e16f14008c35dc1b281bd5fc0e": "ALPHA-USDC"
}

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
    return ['asset','size','price','PnL','id','timestamp']

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
    # output.headers["Content-Disposition"] = "attachment; filename=export.csv"
    # output.headers["Content-type"] = "text/csv"
    return output

@app.route('/')
def hello():
    return 'Hello World'

@app.route('/api/funding', methods=['GET'])
def return_funding():
    if 'address' in request.args:
        address = request.args['address']
    else:
        return "Error: Address not specified"
    return address

@app.route('/api/trades', methods=['GET'])
def return_trades():
    if 'address' in request.args:
        address = request.args['address']
    else:
        return "Error: Address not specified"
    trades = get_all_trades(address)
    return trades_to_csv(trades)
