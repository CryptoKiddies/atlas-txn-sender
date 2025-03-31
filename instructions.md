## Context

Helius maintains an open-source project called Atlas Transaction Sender. It’s an optimized implementation of the `sendTransaction` Solana RPC method. It accepts customer transactions and forwards them to the upcoming Solana leaders (block producers).

Atlas’ tenants:

- Minimize latency – allow customers to land their transactions as quickly as possible
- Maximize redundancy – never drop customer transactions
- Minimize bandwidth – avoid sending invalid transactions (e.g. already confirmed)

https://github.com/helius-labs/atlas-txn-sender

## Objective

Implement a new RPC method within Atlas, called `sendTransactionBundle`.

The new method should meet the following criteria:

- Accept an array of transactions
- Transactions must execute serially
  - A transaction should only be submitted if the previous transaction is confirmed and successful.
  - If a transaction is reverted (failed on-chain), no further transactions are submitted.
- Adhere to Atlas’ tenants
- Bonus: further reduce bandwidth (send less invalid transactions)
  - Hint: blockhashes

## Submission

1. Create a **private** fork of our repository.
2. Grant access to [Nick](https://github.com/nicolaspennie) and [Liam](https://github.com/vovkman).
3. Provide Joe McMullin with the URL
4. Push your changes to a branch.
5. Open a pull request with your changes.
   1. List assumptions you have taken (if any)
   2. Explain the limitations of your solution (if any)

The assessment should take between 1-8 hours to complete, depending on your familiarity with Solana. You are allowed to spend more time on the solution. You have one week to complete the assessment.

## General Information

- Don’t be discouraged if you are new to Solana. Evaluation is adjusted based on your pre-existing Solana and crypto experience (if any).
- The assessment is intentionally open-ended. It’s part of the challenge.
- The existing code contains tech debt. That’s intentional. The assignment is meant to reflect real world scenarios.
- We will discuss your solution in the next interview and ask follow-up questions. So please don’t copy or use external help for this assignment.
- You will need a Helius RPC endpoint. You can create one at https://dashboard.helius.dev for free.
  - In devnet, you can use the [requestAirdrop](https://solana.com/docs/rpc/http/requestairdrop) method to get SOL for testing purposes.
- You can use these gRPC endpoint to stream data:
  - Devnet
    - URL: `https://grpc-takehome-devnet.helius-rpc.com:443`
    - Token: `826dfa33-2a09-4edd-8a49-7a77c0e6fb3b`
    - Warning: May not be available. Devnet gRPC can be unstable.
  - Mainnet
    - URL: `https://grpc-takehome-mainnet.helius-rpc.com:2053`
    - Token: `c1593ae5-1372-47db-afb5-da17edbc1120`
- Useful resources:
  - https://docs.helius.dev/
  - https://solana.com/docs
  - https://solana.com/developers/cookbook
  - https://www.helius.dev/blog
- Use the Helius Discord to ask Solana-related questions.
- Be creative and have fun with it!

airdrop req:
curl https://api.devnet.solana.com -s -X \
 POST -H "Content-Type: application/json" -d '
{
"jsonrpc": "2.0",
"id": 1,
"method": "requestAirdrop",
"params": [
"83astBRguLMdt2h5U1Tpdq5tjFoJ6noeGwaY3mDLVcri",
1000000000
]
}
'
