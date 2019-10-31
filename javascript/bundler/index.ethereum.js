const { createDfuseClient } = require("@dfuse/client")

const client = createDfuseClient({
  apiKey: process.env.DFUSE_API_KEY,
  network: "mainnet.eth.dfuse.io",
})

// You would normally use your framework entry point and render using components,
// we are using pure HTML manipulation for sake of example simplicity.
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
        const paragraphNode = document.createElement("li")
        // Ensure you correctly with the `undo` field
        paragraphNode.innerText = `Transfer ${caller} -> ${address} [${value} Ether]${undo ? " REVERTED" : ""}`

        document.body.prepend(paragraphNode)
      })

      // Mark stream at cursor location, on re-connect, we will start back at cursor
      stream.mark({ cursor })
    }

    if (message.type === "error") {
      const { errors, terminal } = message
      const paragraphNode = document.createElement("li")
      paragraphNode.innerText = `An error occurred ${JSON.stringify({ errors, terminal })}`

      document.body.prepend(paragraphNode)
    }

    if (message.type === "complete") {
        const paragraphNode = document.createElement("li")
        paragraphNode.innerText = "Completed"

        document.body.prepend(paragraphNode)
    }
  })

  // Waits until the stream completes, or forever
  await stream.join()
  await client.release()
}

main().catch((error) => document.body.innerHTML = `<p>${error}</p>`)
