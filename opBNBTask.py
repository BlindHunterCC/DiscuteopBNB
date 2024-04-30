import random
import string
import sys

import asyncio
from typing import Union

from eth_account import Account
from web3 import AsyncWeb3, AsyncHTTPProvider
from loguru import logger

logger.remove()
logger.add(sys.stdout, colorize=True, format="<g>{time:HH:mm:ss:SSS}</g> | <c>{level}</c> | <level>{message}</level>")

abi = [{"inputs": [], "stateMutability": "nonpayable", "type": "constructor"}, {
    "inputs": [{"internalType": "address[]", "name": "_to", "type": "address[]"},
               {"internalType": "uint256[]", "name": "_amount", "type": "uint256[]"}], "name": "distributeBNB",
    "outputs": [], "stateMutability": "payable", "type": "function"},
       {"inputs": [], "name": "owner", "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view", "type": "function"},
       {"inputs": [], "name": "withdrawAll", "outputs": [], "stateMutability": "nonpayable", "type": "function"},
       {"stateMutability": "payable", "type": "receive"}]
w3 = AsyncWeb3(AsyncHTTPProvider('https://opbnb-mainnet-rpc.bnbchain.org'))
contract_address = '0x491949148c17Eed0734863B3F300C5dD26ACf3Ad'
#owner_address = w3.to_checksum_address('0xe413A67Ed9D29341BEE0C7aEa4033EB3E842D513')
#owner_key = 'e160b3ad0e56ced7e22632b4089c864626f9b203ded9ed8ffcf1638ec6f9755b'


class BnbChainDistribute(object):
    def __init__(self, rpc_url):
        self.rpc_url = rpc_url
        self.w3 = AsyncWeb3(AsyncHTTPProvider(self.rpc_url))

    async def check_balance(self, address):
        account_balance = await self.w3.eth.get_balance(self.w3.to_checksum_address(address.strip()))
        logger.info(f" Account {address} balance result: {self.w3.from_wei(account_balance, 'ether')}")

    async def check_nonce(self, address):
        account_nonce = await self.w3.eth.get_transaction_count(self.w3.to_checksum_address(address.strip()))
        logger.info(f" Account {address} nonce: {account_nonce}")


async def check_balance(address,addresses_has_no_bnb):
    account_balance = await w3.eth.get_balance(w3.to_checksum_address(address))
    account_balance = w3.from_wei(account_balance, 'ether')
    if account_balance < 0.0005:
        addresses_has_no_bnb.append(address)
    logger.info(f" Account {address} balance result: {account_balance}")
    return account_balance, f"{address},{account_balance}"

async def distribute(addresses,owner_key):
    owner_account = w3.eth.account.from_key(owner_key)
    owner_address = owner_account.address
    contract = w3.eth.contract(address=w3.to_checksum_address(contract_address), abi=abi)
    contract_balance = await w3.eth.get_balance(w3.to_checksum_address(contract_address))
    logger.info(f" Contract balance: {w3.from_wei(contract_balance, 'ether')} BNB")
    amounts = [w3.to_wei(round(random.uniform(0.0012, 0.002), 5), 'ether') for _ in addresses]
    # amounts = [w3.to_wei(0.003, 'ether') for _ in addresses]
    tx = {
        "from": owner_address,
        "chainId": 204,
        'gas': 0,
        "nonce": await w3.eth.get_transaction_count(owner_address),
    }
    try:
        estimate = await w3.eth.estimate_gas(tx)
        logger.info(f"estimated gas: {w3.from_wei(estimate, 'ether')}")
        tx['gas'] = int(estimate * len(amounts))
    except Exception as e:
        raise Exception(f'Tx simulation failed: {str(e)}')
    txn = await contract.functions.distributeBNB(addresses, amounts).build_transaction(tx)
    signed_tx = w3.eth.account.sign_transaction(txn, owner_key)
    signed_txn = await w3.eth.send_raw_transaction(signed_tx.rawTransaction)
    receipt = await w3.eth.wait_for_transaction_receipt(signed_txn.hex())
    if receipt.status != 1:
        print(f"Transaction {signed_txn.hex()} failed!")
        await asyncio.sleep(5)
    logger.info(f"Transaction success {signed_txn.hex()}")
    await asyncio.sleep(5)


# async def withdraw(owner_address):
#     contract = w3.eth.contract(address=w3.to_checksum_address(contract_address), abi=abi)
#     contract_balance = await w3.eth.get_balance(w3.to_checksum_address(contract_address))
#     logger.info(f" Contract balance: {w3.from_wei(contract_balance, 'ether')} BNB")
#     tx = {
#         "from": owner_address,
#         "chainId": 204,
#         'gas': 0,
#         "nonce": await w3.eth.get_transaction_count(owner_address),
#     }
#     try:
#         estimate = await w3.eth.estimate_gas(tx)
#         logger.info(f"estimated gas: {w3.from_wei(estimate, 'ether')}")
#         tx['gas'] = int(estimate * 1.1)
#     except Exception as e:
#         raise Exception(f'Tx simulation failed: {str(e)}')
#     txn = await contract.functions.withdrawAll().build_transaction(tx)
#     signed_tx = w3.eth.account.sign_transaction(txn, owner_key)
#     signed_txn = await w3.eth.send_raw_transaction(signed_tx.rawTransaction)
#     receipt = await w3.eth.wait_for_transaction_receipt(signed_txn)
#     if receipt.status != 1:
#         print(f"Transaction {signed_txn.hex()} failed!")
#         await asyncio.sleep(5)
#         return
#     logger.info(f"Transaction success {signed_txn.hex()}")
#     await asyncio.sleep(5)


async def process_task(semaphore, addresses, owner_keys,success_file, fail_file):
    addresses = [address.strip() for address in list(addresses)]
    addresses = [addresses[i:i+3] for i in range(0,len(addresses),3)]    
    async with semaphore:
        for recaddresses,owner_key in zip(addresses,owner_keys):
            owner_key = owner_key.strip()
            addresses_has_no_bnb = []
            for address in recaddresses:
                account_balance, log = await check_balance(address.strip(), addresses_has_no_bnb)
                success_file.write(f'{log}\n')
                success_file.flush()
            await distribute(addresses_has_no_bnb,owner_key)
            for address in addresses_has_no_bnb:
                    account_balance, log = await check_balance(address)
                    success_file.write(f'{log}\n')
                    success_file.flush()

async def write_success_file(key: Union[str, int]):
    with open('bnb_distribute_success.csv', 'a+') as f:
        f.write(f'{key}\n')


async def main():
    semaphore = asyncio.Semaphore(int(1))  # 限制并发量
    with open('op-receiver-address.txt', 'r') as addresses,open('op-owners-address.txt', 'r') as owner_keys:
        with open('task_success.csv', 'a+') as success_file, open('task_fail.txt', 'a+') as fail_file:
            task = await process_task(semaphore, addresses, owner_keys,success_file, fail_file)


if __name__ == '__main__':
    asyncio.run(main())
