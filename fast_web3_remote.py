## this uses 
import itertools
import json
import time
from datetime import datetime #, timedelta

t = "--fast_web3_remote "

#from config import rpc_path_remote
#rpc_path_local = '~/.ethereum/geth.ipc'
#ipc_path_chosen = rpc_path_remote              #Config here the path you want to use
#print (f"\nremote= {rpc_path_remote} \nlocal={rpc_path_local} \nconnecting to= {ipc_path_chosen}\n\n")

from config import RPCconfig
if RPCconfig.rpc_location == "REMOTE":
    from web3 import Web3, EthereumTesterProvider
    rpc_path = RPCconfig.rpc_path_remote

elif RPCconfig.rpc_location == "LOCAL":
    import os
    import socket
    rpc_path = RPCconfig.rpc_path_local
    
print(f"FastWeb3 - using {RPCconfig.rpc_location} rpc_path: {rpc_path}")

class FastWeb3:
    def __init__(self,timeout=1):
        self.remote = True if RPCconfig.rpc_location == "REMOTE" else False
        self.ipc_path = rpc_path
        print(f"FastWeb3 - __init__ {RPCconfig.rpc_location} = {self.ipc_path}")
        if "~/" in self.ipc_path:
            self.connect_local_node()
        else:
            self.connect_remote_node()
        

    def connect_remote_node(self, timeout=1):
        print(f"connect to remote node at rpc_path: {self.ipc_path}")
        self.w3 = Web3(Web3.HTTPProvider(self.ipc_path))
        
    def connect_local_node(self, timeout=1):
        if "~/" in self.ipc_path:
            self.ipc_path = os.path.expanduser(self.ipc_path)
        print (f"connecting to localnode ipc_path: {self.ipc_path}")
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.ipc_path)
        self.sock.settimeout(timeout)
        self.request_counter = itertools.count()


    def get_latest_block_number(self):
        if self.remote:
            num = self.get_latest_block()['number']
            #print(f"- lastest block number: {num}")
            return num
        else:
            block_hash = self.get_latest_block()['result']['hash']
            block_number = self.get_block_by_hash([block_hash])['result']['number']
            return int(block_number, 16)

    def get_latest_block(self):
        if self.remote:
            latest_block = self.w3.eth.get_block('latest')
            return latest_block
        else:
            params = f'["latest",false]'
            request = self.make_request('eth_getBlockByNumber', params)
            self.sock.sendall(request)
            return self.batch_receive_response(1)[0]

    def get_block_by_number(self, block_number):
        if self.remote:
            # theoretically the get_block has a full_transaction Boolean as second parameter that needs to be True
            # but apparently we're already getting the TX list 
            # https://web3py.readthedocs.io/en/stable/web3.eth.html#web3.eth.Eth.get_block
            block = self.w3.eth.get_block(block_number)
            return block
        else:
            return self.batch_get_block_by_number(block_number)[0]
        

    def batch_get_block_by_number(self, block_numbers):
        if self.remote:
            # perform the batch call to the remote node and return the data
            # you need to format the responses similar to how block_index.build_rows expects it
            ''' 
                block_number = response['id']
                block = response['result']
                    block['timestamp']
                    block['miner']
                    block['extraData']

            this looks like a dictionary
            responses = [
                {
                    "id": block_number_here,
                    "result": {
                        "timestamp":  timestamp_here,
                        "miner":      miner_here,
                        "extraData":  extraData_here
                    }
                }, 
                {..anotherResponse}
            ]

            response = [
                {
                    "id": b.number,
                    "result": {
                        "timestamp":    b.timestamp,
                        "miner":        b.miner,
                        "extraData":    b.extraData,
                        "difficulty":   b.difficulty,
                        "gasUsed":      b.gasUsed,
                        "transactions"  b.transactions
                    }
                }
            ]
    
            in the local version it's FastWeb3.batck_receive_responses function
            that handles the reception and formats it for the above function with this code
                responses = raw_response.split(b'\n')[:-1]
                data = [json.loads(e) for e in responses]

            '''
            responses = []
            for block_number in block_numbers:
                block = self.get_block_by_number(block_number)
                '''response = {
                        "id": block.number,
                        "result": {
                            "timestamp":    block.timestamp,
                            "miner":        block.miner,
                            "extraData":    block.extraData,
                            "difficulty":   block.difficulty,
                            "gasUsed":      block.gasUsed,
                            "txCount":      len(block.transactions)
                            "transactions": block.transactions,
                        }
                    }
                '''
                response = self.get_response_format_from_block(block)
                responses.append(response)
            return responses

        # For Local Geth Node
        else:
            # this function just sends out all the requests
            # but it's the batch_receive_responses that listens on sock and handles the response
            for block_number in block_numbers:
                params = f'["{hex(block_number)}",false]'
                request = self.make_request('eth_getBlockByNumber', params, id=block_number)
                self.sock.sendall(request)
            return self.batch_receive_response(len(block_numbers))


    def batch_get_blocks_and_txs(self, block_numbers, parseTXs=False):
        if self.remote:
            responses_blocks = []
            responses_txs = []
            start_time = time.time()

            for block_number in block_numbers:
                self.print_time_estimate(block_numbers[0], block_numbers[-1], block_number, start_time)
                block = self.get_block_by_number(block_number)
                response_block = self.get_response_format_from_block(block)
                responses_blocks.append(response_block)

                if self.block_has_txs(block):
                    for tx_hash in block.transactions:                        
                        if parseTXs:
                            #extract all TXs and information
                            tx = self.w3.eth.get_transaction(tx_hash)
                            response_tx = self.get_response_format_from_tx(tx)
                        else:
                            #extract only the TX hash and the block without retrieving all the TX data
                            tx = tx_hash
                            response_tx = self.get_response_format_from_tx(tx, block_number)
                        
                        responses_txs.append(response_tx)   
                    #print(f"{t} block {block_number} of {self.get_printable_date(block)} - had {len(block.transactions)} txs - parsed in {time.time() - start_time} seconds")
                    #print(f"{t} responses_txs: {responses_txs}")
            return responses_blocks, responses_txs


    # to be called each time a new block or TX is parsed
    # it requires you to create a variable start_time = time.time() outside the loop 
    # and call this function inside each for in cycle
    def print_time_estimate(self, start_block, end_block, current_block, start_time, update_rate=60):
        duration = time.time() - start_time
        diff = duration % update_rate
        last_diff = 0
        if diff < last_diff:
            remaining_blocks = end_block - current_block
            processed_blocks = current_block - start_block
            remaining_duration = (remaining_blocks / processed_blocks) * duration
            print(f'#{current_block} - remaining {remaining_duration:.1f}s - = {datetime.timedelta(seconds = remaining_duration)}')
        last_diff = diff

    # utility function that formats the response from a remote note in the same way
    # that index.insert_blocks(responses) expects it
    def get_response_format_from_block(self, block):
        response = {
            "id": block.number,
            "result": {
                "timestamp":    block.timestamp,
                "miner":        block.miner,
                "extraData":    block.extraData,
                "difficulty":   block.difficulty,
                "gasUsed":      block.gasUsed,
                "txCount":      len(block.transactions),   #superfluous but let's store it
                "transactions": block.transactions
            }
        }
        #print(f"\n\n{t}retrieved block {block.number}")
        #print(f"{t} type(response['result']['extraData'] = {type(response['result']['extraData'])}")# = <class 'hexbytes.main.HexBytes'>
        #print(f"{t} response['result']['extraData'] = {response['result']['extraData']}")           # outputs already decoded >> b'Geth/v1.0.0-0cdc7647/linux/go1.4'
        #print(f"{t} block.extraData = {block.extraData}")                                           # outputs already decoded >> b'Geth/v1.0.0-0cdc7647/linux/go1.4'
        return response


    ''' this how the table is constructed
    REATE TABLE IF NOT EXISTS transactions (\
            hash BLOB, \
            block_number INTEGER , \
            _from BLOB ,\
            _to BLOB ,\
            gas INTEGER
    '''
    def get_response_format_from_tx(self, tx, block_number=None):
        if hasattr(tx, 'from'):
            # it's a full transaction
            response = {
                'block_number': tx['blockNumber'],
                'hash': tx['hash'],
                '_from': tx['from'],
                '_to': tx['to'],
                'gas': tx['gas']
            }
        else:
            #the tx param is just a hash
            response = {
                'block_number': block_number, 
                'hash': tx,
            }
        #print(f"{t} - get_response_format_from_tx: {response}")
        return response


    def block_has_txs(self, block):
        return ('transactions'in block) and ( len(block.transactions) > 0)
    #utility function to check wither a block response packs Txs
    ##def block_response_has_txs(block_response):
        #return ('transactions' in block_response['result']) and (len(block_response['result']['transactions']) >0)

    def get_printable_date(self, block):
        return datetime.utcfromtimestamp( block.timestamp ).strftime('%Y-%m-%d %H:%M:%S')


# (ACHTUNG this is a secret - obscure before committing or put in an ENV or secrets file that is ignored)
class FastWeb3:
    #def __init__(self, ipc_path='~/.ethereum/geth.ipc', timeout=1):
    def __init__(self, timeout=1):
        self.ipc_path = ipc_path_chosen
        print(f"FastWeb3 __init__ ipc_path={self.ipc_path}")
        if "~/" in self.ipc_path:
            self.connect_local_node()
        else:
            self.connect_remote_node()
        

    def connect_remote_node(self, timeout=1):
        print(f"connect to remote node at ipc_path: {self.ipc_path}")
    
    def connect_local_node(self, timeout=1):
        if "~/" in self.ipc_path:
            ipc_path = os.path.expanduser(ipc_path)
        print (f"connecting to localnode ipc_path: {ipc_path}")
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(ipc_path)
        self.sock.settimeout(timeout)
        self.request_counter = itertools.count()

        
    def make_request(self, method, params, id=None):
        if id is None:
            id = next(self.request_counter)
        return f'{{"jsonrpc":"2.0","method":"{method}","params":{params},"id":{id}}}'.encode('ascii')
        
    def batch_receive_response(self, total):
        raw_response = b''
        remaining_timeouts = 5
        while total > 0:
            try:
                chunk = self.sock.recv(4096)
                raw_response += chunk
                total -= chunk.count(b'\n')
            except socket.timeout:
                remaining_timeouts -= 1
                if remaining_timeouts == 0:
                    raise
        responses = raw_response.split(b'\n')[:-1]
        data = [json.loads(e) for e in responses]
        return data
    
    def get_block_by_number(self, block_number):
        return self.batch_get_block_by_number(block_number)[0]
        
    def batch_get_block_by_number(self, block_numbers):
        for block_number in block_numbers:
            params = f'["{hex(block_number)}",false]'
            request = self.make_request('eth_getBlockByNumber', params, id=block_number)
            self.sock.sendall(request)
        return self.batch_receive_response(len(block_numbers))
    
    def get_block_by_hash(self, block_hash):
        return self.batch_get_block_by_hash(block_hash)[0]
    
    def batch_get_block_by_hash(self, block_hashes):
        for block_hash in block_hashes:
            params = f'["{block_hash}",false]'
            request = self.make_request('eth_getBlockByHash', params)
            self.sock.sendall(request)
        return self.batch_receive_response(len(block_hashes))
    
    def get_latest_block(self):
        params = f'["latest",false]'
        request = self.make_request('eth_getBlockByNumber', params)
        self.sock.sendall(request)
        return self.batch_receive_response(1)[0]
        
    def get_latest_block_number(self):
        block_hash = self.get_latest_block()['result']['hash']
        block_number = self.get_block_by_hash([block_hash])['result']['number']
        return int(block_number, 16)

    def __del__(self):
        self.sock.close()
#"""        