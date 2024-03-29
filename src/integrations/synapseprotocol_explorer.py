import json
import requests
from urllib.parse import quote


def send_graphql_get_request(
    query, variables, endpoint="https://explorer.omnirpc.io/graphql"
):
    """
    Sends a GET request to a GraphQL endpoint with the provided query and variables.

    :param query: The GraphQL query string.
    :param variables: A dictionary of variables to be included in the query.
    :param endpoint: The URL of the GraphQL endpoint. Defaults to 'https://explorer.omnirpc.io/graphql'.
    :return: The response from the GraphQL server.
    """
    # Convert the query and variables to URL-encoded strings
    query_encoded = quote(query)
    variables_encoded = quote(json.dumps(variables))

    # Construct the URL with the query and variables
    url = f"{endpoint}?query={query_encoded}&variables={variables_encoded}"

    # Send the GET request
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Request failed with status code {response.status_code}")


# Example usage
query = """
query GetBridgeTransactionsQuery($chainIDFrom: [Int], $chainIDTo: [Int], $addressFrom: String, $addressTo: String, $maxAmount: Int, $minAmount: Int, $maxAmountUsd: Int, $minAmountUsd: Int, $startTime: Int, $endTime: Int, $txnHash: String, $kappa: String, $pending: Boolean, $page: Int, $tokenAddressFrom: [String], $tokenAddressTo: [String], $useMv: Boolean) {
 bridgeTransactions(
    chainIDFrom: $chainIDFrom
    chainIDTo: $chainIDTo
    addressFrom: $addressFrom
    addressTo: $addressTo
    maxAmount: $maxAmount
    minAmount: $minAmount
    maxAmountUsd: $maxAmountUsd
    minAmountUsd: $minAmountUsd
    startTime: $startTime
    endTime: $endTime
    txnHash: $txnHash
    kappa: $kappa
    pending: $pending
    page: $page
    useMv: $useMv
    tokenAddressFrom: $tokenAddressFrom
    tokenAddressTo: $tokenAddressTo
 ) {
    ...TransactionInfo
    __typename
 }
}

fragment TransactionInfo on BridgeTransaction {
 fromInfo {
    ...SingleSideInfo
    __typename
 }
 toInfo {
    ...SingleSideInfo
    __typename
 }
 kappa
 pending
 swapSuccess
 __typename
}

fragment SingleSideInfo on PartialInfo {
 chainID
 destinationChainID
 address
 hash: txnHash
 value
 formattedValue
 tokenAddress
 tokenSymbol
 time
 eventType
 __typename
}
"""

variables = {"pending": False, "page": 2000, "useMv": True}

try:
    response = send_graphql_get_request(query, variables)
    print(response)
except Exception as e:
    print(e)
