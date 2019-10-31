package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	pb "github.com/dfuse-io/quickstart-tutorials/pb"
	"github.com/tidwall/gjson"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
)

func getToken(dfuseAPIKey string) (token string, expiration time.Time, err error) {
	reqBody := bytes.NewBuffer([]byte(fmt.Sprintf(`{"api_key":"%s"}`, dfuseAPIKey)))
	resp, err := http.Post("https://auth.dfuse.io/v1/auth/issue", "application/json", reqBody)
	if err != nil {
		err = fmt.Errorf("unable to obtain token: %s", err)
		return
	}

	if resp.StatusCode != 200 {
		err = fmt.Errorf("unable to obtain token, status not 200, got %d: %s", resp.StatusCode, reqBody.String())
		return
	}

	if body, err := ioutil.ReadAll(resp.Body); err == nil {
		token = gjson.GetBytes(body, "token").String()
		expiration = time.Unix(gjson.GetBytes(body, "expires_at").Int(), 0)
	}
	return
}

func streamEthereum(token string) {
	credential := oauth.NewOauthAccess(&oauth2.Token{AccessToken: token, TokenType: "Bearer"})
	transportCreds := credentials.NewClientTLSFromCert(nil, "")
	endpoint := "mainnet.eth.dfuse.io:443"

	conn, err := grpc.Dial(endpoint, grpc.WithPerRPCCredentials(credential), grpc.WithTransportCredentials(transportCreds))
	if err != nil {
		panic(err)
	}

	client := pb.NewGraphQLClient(conn)
	executor, err := client.Execute(
		context.Background(),
		&pb.Request{Query: `subscription {
		  searchTransactions(query: "-value:0 type:call", lowBlockNum: -1) {
			undo cursor
			node { hash matchingCalls { caller address value(encoding:ETHER) } }
		  }
		}`},
	)

	if err != nil {
		panic(err)
	}

	fmt.Println("Ethereum Transfers")
	for {
		resp, err := executor.Recv()
		if err != nil {
			panic(err)
		}

		document := new(EthereumDocument)
		err = json.Unmarshal([]byte(resp.Data), &document)
		if err != nil {
			panic(err)
		}

		result := document.SearchTransactions
		reverted := ""
		if result.Undo {
			reverted = " REVERTED"
		}

		for _, call := range result.Node.MatchingCalls {
			fmt.Printf("Transfer %s -> %s [%s Ether]%s\n", call.Caller, call.Address, call.Value, reverted)
		}
	}
}

type EthereumDocument struct {
	SearchTransactions struct {
		Cursor string
		Undo   bool
		Node   struct {
			Hash          string
			MatchingCalls []struct {
				Caller  string
				Address string
				Value   string
			}
		}
	}
}

func streamEOSIO(token string) {
	credential := oauth.NewOauthAccess(&oauth2.Token{AccessToken: token, TokenType: "Bearer"})
	transportCreds := credentials.NewClientTLSFromCert(nil, "")
	endpoint := "mainnet.eos.dfuse.io:443"

	conn, err := grpc.Dial(endpoint, grpc.WithPerRPCCredentials(credential), grpc.WithTransportCredentials(transportCreds))
	if err != nil {
		panic(err)
	}

	client := pb.NewGraphQLClient(conn)
	executor, err := client.Execute(context.Background(), &pb.Request{Query: `subscription {
      	searchTransactionsForward(query:"receiver:eosio.token action:transfer") {
          undo cursor
          trace { id matchingActions { json } }
        }
	}`})

	if err != nil {
		panic(err)
	}

	fmt.Println("EOS Transfers")
	for {
		resp, err := executor.Recv()
		if err != nil {
			panic(err)
		}

		document := new(EOSIODocument)
		err = json.Unmarshal([]byte(resp.Data), &document)
		if err != nil {
			panic(err)
		}

		result := document.SearchTransactionsForward
		reverted := ""
		if result.Undo {
			reverted = " REVERTED"
		}

		for _, action := range result.Trace.MatchingActions {
			data := action.JSON
			fmt.Printf("Transfer %s -> %s [%s]%s\n", data["from"], data["to"], data["quantity"], reverted)
		}
	}
}

type EOSIODocument struct {
	SearchTransactionsForward struct {
		Cursor string
		Undo   bool
		Trace  struct {
			ID              string
			MatchingActions []struct {
				JSON map[string]interface{}
			}
		}
	}
}

func main() {
	dfuseAPIKey := os.Getenv("DFUSE_API_KEY")
	if dfuseAPIKey == "" {
		panic("you must specify a DFUSE_API_KEY environment variable")
	}

	token, _, err := getToken(dfuseAPIKey)
	if err != nil {
		panic(err)
	}

	proto := ""
	if len(os.Args) >= 2 {
		proto = os.Args[1]
	}

	switch proto {
	case "eosio", "":
		streamEOSIO(token)
	case "ethereum":
		streamEthereum(token)
	}
}
