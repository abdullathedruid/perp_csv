from flask import Flask, jsonify, request
import requests
import json

app = Flask(__name__)

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

def trades_to_csv(trades):
    trades_raw = []
    for trade in trades:
        trades_raw.append('hi')
    return jsonify(trades_raw)

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
