import pandas as pd
from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_abi import abi
from tqdm import tqdm


# because there is no public abi for bscpad contract, hard-coded
KNOWN_METHODS = {'0x379607f5': ['claim', ['uint256']],
                 '0xb64afbe5': ['participate', ['address', 'uint256']],
                 '0xc29c9736': ['setup', ['address[]', 'uint256[]', 'bool[]']]}
CONTRACT_ADDR = "0xbc4457e17ff8bf75b5b48576f850b9928161828d"
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def get_txs(start, end, address, data_dir="./data"):
    participate_data = {"user": [], "amount": [], "block": [], "tx_hash": []}
    claim_data = {"user": [], "amount": [], "block": [], "tx_hash": []}

    for x in tqdm(range(start, end)):
        block = web3.eth.getBlock(x, True)
        for tx in block.transactions:
            if (tx['to'] and tx['to'].lower() == address) or (tx['from'] and tx['from'].lower() == address):
                hash_str = tx['hash'].hex()
                input_data = tx['input']
                method = input_data[:10]

                if method not in KNOWN_METHODS.keys():
                    continue
                method_name = KNOWN_METHODS[method][0]

                if method_name == 'participate':
                    participate_data['user'].append(tx['from'])
                    participate_data['amount'].append(abi.decode(KNOWN_METHODS[method][1], web3.toBytes(hexstr=input_data[10:]))[1])
                    participate_data['block'].append(tx['blockNumber'])
                    participate_data['tx_hash'].append(hash_str)
                elif method_name == 'claim':
                    receipt = web3.eth.getTransactionReceipt(hash_str)
                    transfer_log = None
                    for log in receipt['logs']:
                        if log['topics'][0] == web3.toBytes(hexstr=TRANSFER_TOPIC):
                            transfer_log = log
                            break
                    if transfer_log is None:
                        continue
                    claim_data['user'].append(tx['from'])
                    claim_data['amount'].append(abi.decode(['uint256'], web3.toBytes(hexstr=transfer_log['data']))[0])
                    claim_data['block'].append(tx['blockNumber'])
                    claim_data['tx_hash'].append(hash_str)

    print(f"from {start} to {end}, there are {len(participate_data['user'])} particiation txs")
    print(f"from {start} to {end}, there are {len(claim_data['user'])} claim txs")

    if len(participate_data['user']) > 0:
        pd.DataFrame(participate_data).to_csv(f"{data_dir}/participates_{address}_{start}_{end}.csv", index=False)
    if len(claim_data['user']) > 0:
        pd.DataFrame(claim_data).to_csv(f"{data_dir}/claims_{address}_{start}_{end}.csv", index=False)


if __name__ == "__main__":
    endpoint = "https://bsc-dataseed.binance.org/"
    web3 = Web3(Web3.HTTPProvider(endpoint))
    web3.middleware_onion.inject(geth_poa_middleware, layer=0)
    if not web3.isConnected():
        print("not connected to web3")
        exit()

    """
    bscscan_endpoint = "https://api.bscscan.com/api"
    contract_address = web3.toChecksumAddress(contract_addr)
    API_KEY = "<ASDFASDFASDF>"
    API_ENDPOINT = f"{bscscan_endpoint}?module=contract&action=getabi&address={contract_address}&apikey={API_KEY}"
    r = requests.get(url = API_ENDPOINT)
    response = r.json()
    abi = json.loads(response['result'])
    print(abi)
    """

#    start_block = 23271877 - 1
    start_block = 16046507 - 1
    end_block = start_block + 1000
#    end_block = web3.eth.blockNumber

    get_txs(start_block, end_block, CONTRACT_ADDR)



