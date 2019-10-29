package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
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
	if err != nil || resp.StatusCode != 200 {
		err = fmt.Errorf("status code: %d, error: %s", resp.StatusCode, err)
		return
	}
	if body, err := ioutil.ReadAll(resp.Body); err == nil {
		token = gjson.GetBytes(body, "token").String()
		expiration = time.Unix(gjson.GetBytes(body, "expires_at").Int(), 0)
	}
	return
}

func queryETH(token string) {
	// Authorization: bearer {token}
	credential := oauth.NewOauthAccess(
		&oauth2.Token{AccessToken: token, TokenType: "Bearer"},
	)

	// Enable SSL
	transportCreds := credentials.NewClientTLSFromCert(nil, "")

	// Connect to ETH endpoint
	endpoint := "mainnet.eth.dfuse.io:443"
	conn, err := grpc.Dial(
		endpoint,
		grpc.WithPerRPCCredentials(credential),
		grpc.WithTransportCredentials(transportCreds),
	)
	if err != nil {
		panic(err)
	}

	client := pb.NewGraphQLClient(conn)
	executor, err := client.Execute(
		context.Background(),
		&pb.Request{Query: `subscription
      {
       searchTransactions(query: "method:\"transfer(address,uint256)\"",
	     sort:DESC, limit:1) {
         node{
           hash
         }
         block{
           number
         }
       }
     }`},
	)
	if err != nil {
		panic(err)
	}

	resp, err := executor.Recv()
	if err != nil {
		panic(err)
	}
	fmt.Println("ETH Mainnet last transfer", resp.Data)
}

func queryEOS(token string) {
	// Authorization: bearer {token}
	credential := oauth.NewOauthAccess(&oauth2.Token{AccessToken: token, TokenType: "Bearer"})

	// Enable SSL
	transportCreds := credentials.NewClientTLSFromCert(nil, "")

	// Connect to ETH endpoint
	endpoint := "mainnet.eos.dfuse.io:443"
	conn, err := grpc.Dial(endpoint, grpc.WithPerRPCCredentials(credential), grpc.WithTransportCredentials(transportCreds))
	if err != nil {
		panic(err)
	}
	client := pb.NewGraphQLClient(conn)
	executor, err := client.Execute(context.Background(), &pb.Request{Query: `subscription {
searchTransactionsForward(query:"receiver:eosio.token action:transfer", limit:1){
  block{
    num
  }
  trace{
    id
  }
}
}
`})
	if err != nil {
		panic(err)
	}

	resp, err := executor.Recv()
	if err != nil {
		panic(err)
	}
	fmt.Println("EOS last transfer: ", resp.Data)
}

func main() {
	dfuseAPIKey := os.Getenv("DFUSE_API_KEY")
	if dfuseAPIKey == "" {
		panic("no DFUSE_API_KEY env var")
	}

	token, _, err := getToken(dfuseAPIKey)
	if err != nil {
		panic(err)
	}

	proto := strings.ToLower(os.Getenv("DFUSE_PROTO"))
	if proto == "eth" || proto == "" {
		queryETH(token)
	}
	if proto == "eos" || proto == "" {
		queryEOS(token)
	}

}
