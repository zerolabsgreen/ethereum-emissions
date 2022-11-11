t = "---block_index_remote "

import sqlite3
import datetime

from config import RPCconfig
remote = True if RPCconfig.rpc_location == "REMOTE" else False


def hash0x_to_bytes(hash0x):
    #print(f"{t} hash0x_to_bytes  type(hash0x): {type(hash0x)}")
    #print(f"{t} hash0x_to_bytes  hash0x: {hash0x}")
    # block0 causes problems because it returns this warning TypeError: fromhex() argument must be str, not HexBytes
    # so we convert it to string
    if not isinstance(hash0x, str):
        hash0x = hash0x.hex()
        #print(f"{t} hash0x_to_bytes  type(hash0x) 2: {type(hash0x)}")
        #print(f"{t} hash0x_to_bytes  hash0x 2: {hash0x}")
        
    return bytearray.fromhex(hash0x[2:])


# extract each TX as a single Row + Query the info of who is it from and the rest
# feels a bit redundant, but let's follow how block rows are build from their responses
def build_txs_rows(responses):
    for tx in responses:
        if '_from' in tx:
            # hash needs to be converted because if I just put the tx['hash'] the resulting SQLite will be empty
            #tx_hash = hash0x_to_bytes(tx['hash'])# still empty
            #store all hashes as bytes - they are more compact, so better than just tx['_from'] 
            tx_hash = hash0x_to_bytes(tx['hash'].hex())
            _from   = hash0x_to_bytes(tx['_from'].hex())  # tx['_from']
            _to     = hash0x_to_bytes(tx['_to'].hex())    # tx['_to']
            yield(
                tx['block_number'],
                tx_hash,
                _from,
                _to,
                tx['gas']
            ) 
        else:
            # it's just the TX and block Number
            yield (
                tx['block_number'],
                hash0x_to_bytes(tx['hash'].hex())
            )


def build_rows(responses):
    #print(f"{t} build_rows responses[0].['result']['extraData']: {responses[0]['result']['extraData']}")
    #print(f"{t} build_rows responses[1].['result']['extraData']: {responses[1]['result']['extraData']}")
    for response in responses:
        block_number = response['id']
        #print(f"\n\n{t} build_rows parsing block: {block_number}")
        block = response['result']

        # Kyle converts the timestamp to hex like this
        # int(block['timestamp'], 16),
        # but the block['timestamp'] from remote node is already of type int so no need to convert it
        timestamp = block['timestamp']
        #print(f"timestamp: {timestamp} / type: {type(timestamp)}  /  = {datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')}")

        '''miner = '0x05a56E2D52c817161883f50c441c3228CFe54d9f'
        # bytearray.fromhex(b1.miner[2:])
        # outputs bytearray(b'\x05\xa5n-R\xc8\x17\x16\x18\x83\xf5\x0cD\x1c2(\xcf\xe5M\x9f')
        # why store it as bytearray rather than the simple string or the hex??
        # maybe this is a hint.. https://www.alpharithms.com/python-bytearray-built-in-function-123516/
        # it's probably for storage purposes, deleting the 0x deletes 2 bytes for millions of blocks and datapoints = it can add to GB
        # maybe it's for better storage in SQLite
        # but from his code it doesn't make sense
        # in BlockClassifier.classify_miner(miner): - he converts it back to hex and adds the "0x" to it. 
        #   miner = '0x' + miner.hex()
        '''
        miner = hash0x_to_bytes(block['miner']) #this is how Kyle stores it but it creates a conflict with the BLOB type in SQLite
        #miner = block['miner'] # this doesn't create any problems now - but writes the 0x as a string = more bytes + will this create problems later?

        ''' the extraData is already of type HexBytes
        # HexBytes('0x476574682f76312e302e302f6c696e75782f676f312e342e32')
        # which you could decode directly by reading it 
        # print(f"{t} type(block['extraData']): {type(block['extraData'])}")
        #print(f"{t} block['extraData']: {block['extraData']}") #outputs b'Geth/v1.0.0-0cdc7647/linux/go1.4'    or for Block 0 it outputs the bytes
        ## print(f"{t} block:{block_number}  {block['extraData'].decode()}")# causes problem because block['extraData'] it's already decoded 
        '''
        extraData = hash0x_to_bytes(block['extraData']) #this is how kyle does it but it is visible as a string in SQLite (not sure if it's just the DB UI though)
        #print(f"{t} kyle extraData in bytes: {extraData}")  #outputs bytearray(b'Geth/v1.0.0-0cdc7647/linux/go1.4')
        #extraData = block['extraData'] ## BB let's see what happens when I store this
        
        #transactions = block['transactions'] # add later
        yield(
            block_number,
            timestamp,
            miner,
            extraData,
            block['difficulty'],
            block['gasUsed'],
            block['txCount'],
        )
            
##this was how Kyle stores the data called build_rows - converts all hashes to bytes
"""def build_rows(responses):
    for response in responses:
        block_number = response['id']
        block = response['result']
        yield (
            block_number,            
            int(block['timestamp'], 16),
            hash0x_to_bytes(block['miner']),
            hash0x_to_bytes(block['extraData'])
        )
#"""     

 

def decode_extra_data(e):
    try:
        return e.decode('utf-8')
    except:
        return e.decode('latin-1')


class Block:
    ##BB add here the parameters that we need - 
    # gas_used? - to assign to this block a % of the days's energy consumption? > maybe better to use difficulty?
    # difficulty?
    # maybe transactions if we want to do a per TX split - maybe it's enough the Nr 
    # block_number is enough to get the etherscan link https://etherscan.io/block/15410580
    def __init__(self, block_number, timestamp, miner, extra_data, difficulty, gasUsed, txCount):
        self.block_number = block_number
        self.timestamp = timestamp
        self.miner = miner
        self.extra_data = extra_data
        self.difficulty = difficulty
        self.gasUsed = gasUsed
        self.txCount = txCount

    def __repr__(self):
        return str(self.block_number)

    def extra_data_decoded(self):
        return decode_extra_data(self.extra_data)

    def get_datetime(self):
        return datetime.datetime.fromtimestamp(self.timestamp)
    
    def get_block_number(self):
        return self.block_number




class BlockIndex:
    def __init__(self, db_file='cache/block_index.sqlite3', read_only=False, table='extra_data'):
        self.table = table
        self.create_block_table(db_file, read_only, table)
        self.create_tx_table(db_file, read_only, table)



    def create_block_table(self, db_file='cache/block_index.sqlite3', read_only=False, table='extra_data'):
        flags = '?mode=ro' if read_only else ''
        self.db = sqlite3.connect(f'file:{db_file}{flags}', uri=True)
        # self.db.execute('PRAGMA journal_mode=wal')
        cmd = f'CREATE TABLE IF NOT EXISTS {table} \
            (block_number INTEGER PRIMARY KEY, \
            timestamp INTEGER, \
            miner BLOB, \
            extra_data BLOB ,\
            difficulty INTEGER, \
            gasUsed	INTEGER, \
            txCount INTEGER \
            )'
            #BB add here the fields that are needed in the table
        self.db.execute(cmd)

    def create_tx_table(self, db_file='cache/block_index.sqlite3', read_only=False, table='extra_data'):
        flags = '?mode=ro' if read_only else ''
        self.db = sqlite3.connect(f'file:{db_file}{flags}', uri=True)
        #id INTEGER PRIMARY KEY , \
        cmd = 'CREATE TABLE IF NOT EXISTS transactions (\
            id INTEGER, \
            block_number INTEGER, \
            hash BLOB, \
            _from BLOB ,\
            _to BLOB ,\
            gas INTEGER, \
            PRIMARY KEY("id" AUTOINCREMENT) \
            );'
            #BB add here the fields that are needed in the table
        self.db.execute(cmd)
        
        
    def __del__(self):
        self.db.close()
        
    def execute(self, query):
        return self.db.cursor().execute(query)

    def list_field(self, field, ordered=False):
        query = f'SELECT {field} FROM {self.table}'
        for row in self.execute(query):
            yield row[0]

    def list_field_unique(self, field):
        query = f'SELECT DISTINCT {field} FROM {self.table}'
        for row in self.execute(query):
            yield row[0]
    

    ## test if it has the 'transactions' key and if it's not empty >> could use the txCount variable too
    def has_transactions(block_response):
        return ('transactions' in block_response['result']) and (len(block_response['result']['transactions']) >0)


    def insert_blocks(self, responses):
        if remote:
            ''' 
                extra_data > block fields
                block_number,
                timestamp,
                miner,
                extraData,
                block['difficulty'],
                block['gasUsed'],
                block['txCount'],
                #block['transactions'] ## these can't go in SQLite so you need to write it to another table
            '''
            
            query = f'INSERT OR REPLACE INTO {self.table} VALUES (?, ?, ?, ?, ?, ?, ?)'
            self.db.cursor().executemany(query, build_rows(responses))
            self.db.commit() 

            # if it has  add those as well to a different table
            

        else:
            query = f'INSERT OR REPLACE INTO extra_data VALUES (?, ?, ?, ?)'
            self.db.cursor().executemany(query, build_rows(responses))
            self.db.commit()


    '''responses can be of 2 types
    responses = [{ hash, block_number, _from, _to, gas},{}] #full
    responses = [{ hash, block_number}, {}]                 #simple
    '''
    ''' this how the transactions table is constructed 
            REATE TABLE IF NOT EXISTS transactions (\
                hash BLOB PRIMARY KEY, \
                block_number INTEGER , \
                _from BLOB ,\
                _to BLOB ,\
                gas INTEGER
            '''
    def insert_transactions(self, responses):
        if remote:
            if '_from' in responses[0]:
                #it's a full tx - build the query for all parameters
                query = f'INSERT OR REPLACE INTO transactions(block_number, hash, _from, _to, gas) VALUES (?, ?, ?, ?, ?)'
            else:
                #it's a simple tx - build the query for only 2 parameters
                query = f'INSERT OR REPLACE INTO transactions(block_number, hash) VALUES (?, ?)'
            
            self.db.cursor().executemany(query, build_txs_rows(responses))
            self.db.commit()

    
    def latest_block(self):
        query = f'SELECT MAX(block_number) FROM {self.table}'
        return self.execute(query).fetchone()[0]

    def list_blocks(self, skip_genesis=False):
        query = f'SELECT * FROM {self.table} ORDER BY block_number ASC'
        if skip_genesis:
            query += ' LIMIT -1 OFFSET 1'
        for row in self.execute(query):
            yield Block(*row)

    # you can get a subset of the blocks and from a specified table if you need to
    ## TODO see if you want to delete the table param from here since now you are passing it to the
    def list_blocks_range(self, start_block, end_block, table="extra_data", skip_genesis=False):
        query = f'SELECT * FROM {table} \
            WHERE block_number >= {start_block} and block_number<={end_block} \
            ORDER BY block_number ASC'
        if skip_genesis:
            query += ' LIMIT -1 OFFSET 1'    
        for row in self.execute(query):
            yield Block(*row)

    
    

    def is_block_in_db(self, block_number):
        query = f'SELECT COUNT(1) FROM {self.table} WHERE block_number = {block_number}'
        return self.execute(query).fetchone()[0]  # returns 0 if not found, and 1 if found