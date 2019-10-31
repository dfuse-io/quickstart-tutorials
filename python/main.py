try:
    # python3
    from http.client import HTTPSConnection
except ImportError:
    # python2
    from httplib import HTTPSConnection

import json
import ssl
import sys

import grpc

from graphql import graphql_pb2_grpc
from graphql.graphql_pb2 import Request

def token_for_api_key(apiKey):
    connection = HTTPSConnection("auth.dfuse.io")
    connection.request('POST', '/v1/auth/issue', json.dumps({"api_key": apiKey}), {'Content-type': 'application/json'})
    response = connection.getresponse()

    if response.status != 200:
        raise Exception(" Status: %s reason: %s" % (response.status, response.reason))

    token = json.loads(response.read().decode())['token']
    connection.close()

    return token

def client(endpoint):
    credentials = grpc.access_token_call_credentials(token_for_api_key(sys.argv[1]))
    channel = grpc.secure_channel(endpoint,
                                  credentials=grpc.composite_channel_credentials(grpc.ssl_channel_credentials(),
                                                                                 credentials))
    return graphql_pb2_grpc.GraphQLStub(channel)

def stream_ethereum(client):
    query = """
    subscription {
      searchTransactions(query: "-value:0 type:call", lowBlockNum: -1) {
         undo cursor
         node { hash matchingCalls { caller address value(encoding:ETHER) } }
       }
    }
    """
    dfuse_graphql = client('mainnet.eth.dfuse.io:443')
    stream = dfuse_graphql.Execute(Request(query=query))

    print("ETH Results")
    for rawResult in stream:
        if rawResult.errors:
          print("An error occurred")
          print(rawResult.errors)
        else:
          result = json.loads(rawResult.data)
          for call in result['searchTransactions']['node']['matchingCalls']:
            undo = result['searchTransactions']['undo']
            print("Transfer %s -> %s [%s Ether]%s" % (call['caller'], call['address'], call['value'], " REVERTED" if undo else ""))

def stream_eosio(client):
    operation = '''
      subscription {
        searchTransactionsForward(query:"receiver:eosio.token action:transfer") {
          undo cursor
          trace { id matchingActions { json } }
        }
      }
    '''

    dfuse_graphql = client('mainnet.eos.dfuse.io:443')
    stream = dfuse_graphql.Execute(Request(query=operation))

    print("EOS Transfers")
    for rawResult in stream:
        if rawResult.errors:
          print("An error occurred")
          print(rawResult.errors)
        else:
          result = json.loads(rawResult.data)
          for action in result['searchTransactionsForward']['trace']['matchingActions']:
            undo = result['searchTransactionsForward']['undo']
            data = action['json']
            print("Transfer %s -> %s [%s]%s" % (data['from'], data['to'], data['quantity'], " REVERTED" if undo else ""))

proto = ""
if len(sys.argv) > 2:
    proto = sys.argv[2].lower()

if proto == "ethereum" or proto == "":
  stream_ethereum(client)

if proto == "eosio" or proto == "":
  stream_eosio(client)



