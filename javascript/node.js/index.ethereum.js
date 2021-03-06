global.fetch = require('node-fetch')
global.WebSocket = require('ws')

const { createDfuseClient } = require("@dfuse/client")

const client = createDfuseClient({
  apiKey: process.env.DFUSE_API_KEY,
  network: "mainnet.eth.dfuse.io",
})

async function main() {
  // You must use a `$cursor` variable so stream starts back at last marked cursor on reconnect
  const operation = `subscription($cursor: String!) {
    searchTransactions(query:"-value:0 type:call", lowBlockNum: -1, cursor: $cursor) {
      undo cursor
      node {
        hash matchingCalls { caller address value(encoding:ETHER) }
      }
    }
  }`

  // Goes inside `main` function
  const stream = await client.graphql(operation, (message) => {
    if (message.type === "data") {
      const { undo, cursor, node: { hash, value, matchingCalls }} = message.data.searchTransactions
      matchingCalls.forEach(({ caller, address, value }) => {
        // Ensure you correctly deal with the `undo` field
        console.log(`Transfer ${caller} -> ${address} [${value} Ether]${undo ? " REVERTED" : ""}`)
      })

      // Mark stream at cursor location, on re-connect, we will start back at cursor
      stream.mark({ cursor })
    }

    if (message.type === "error") {
      console.log("An error occurred", message.errors, message.terminal)
    }

    if (message.type === "complete") {
      console.log("Completed")
    }
  })

  // Waits until the stream completes, or forever
  await stream.join()
  await client.release()
}

main().catch((error) => console.log("Unexpected error", error))

