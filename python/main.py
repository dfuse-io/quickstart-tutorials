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
        raise Exception(" Status: {response.status} reason: {response.reason}")

    token = json.loads(response.read().decode())['token']
    connection.close()

    return token

def client(endpoint):
    credentials = grpc.access_token_call_credentials(token_for_api_key(sys.argv[1]))
    channel = grpc.secure_channel(endpoint,
                                  credentials=grpc.composite_channel_credentials(grpc.ssl_channel_credentials(),
                                                                                 credentials))
    return graphql_pb2_grpc.GraphQLStub(channel)

def query_eth(client):
    query = """
    subscription {
      searchTransactions(query: "method:\\"transfer(address,uint256)\\"", limit: 5, sort: DESC) {
         node{
           hash
         }
         block{
           number
         }
       }
    }
    """
    dfuse_graphql = client('mainnet.eth.dfuse.io:443')
    stream = dfuse_graphql.Execute(Request(query=query))

    print("== ETH results ==")
    for rawResult in stream:
        if rawResult.errors:
          print("An error occurred")
          print(rawResult.errors)
        else:
          result = json.loads(rawResult.data)
          print(result['searchTransactions'])

def query_eos(client):
    query = '''
    subscription {
      searchTransactionsForward(query: "action:onblock", limit: 5) {
        trace {
          id
          matchingActions{
            account
            receiver
            name
            json
          }
        }
      }
    }
    '''
    dfuse_graphql = client('mainnet.eos.dfuse.io:443')
    stream = dfuse_graphql.Execute(Request(query=query))

    print("== EOS results ==")
    for rawResult in stream:
        if rawResult.errors:
          print("An error occurred")
          print(rawResult.errors)
        else:
          result = json.loads(rawResult.data)
          print(result['searchTransactionsForward']['trace']['matchingActions'])


proto = ""
if len(sys.argv) > 2:
    proto = sys.argv[2].lower()

if proto == "eth" or proto == "":
  query_eth(client)

if proto == "eos" or proto == "":
  query_eos(client)



