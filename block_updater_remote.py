from emissions_utils import chunks
import time, datetime

from fast_web3_remote import FastWeb3
#from block_index import BlockIndex  ## LOCAL version
from block_index_remote import BlockIndex
from config import ScrapeConfig

t = "\n--updater "

chunk_size = ScrapeConfig.blocks_batch_size  #100
web3 = FastWeb3()
index = BlockIndex()

#block = web3.get_block_by_number(1500)
#print(block);

#block_0 = web3.get_block_by_number(0)
#print(f"{t} block0:\n {block_0}")

#9193266

def kyle_updater():
    latest_block_number = web3.get_latest_block_number()
    end_block = latest_block_number
    indexed_block = index.latest_block()
    start_block = 0 if indexed_block is None else indexed_block
    n = end_block - start_block
    start_time = time.time()
    last_diff = 0
    update_rate = 60

    print(f'syncing #{start_block} to #{end_block}')
    for block_numbers in chunks(range(start_block, latest_block_number), chunk_size):
        block_number = block_numbers[0]
        duration = time.time() - start_time
        diff = duration % update_rate
        if diff < last_diff:
            remaining_blocks = end_block - block_number
            processed_blocks = block_number - start_block
            remaining_duration = (remaining_blocks / processed_blocks) * duration
            print(f'#{block_number} {remaining_duration:.1f}s')
        last_diff = diff

        responses = web3.batch_get_block_by_number(block_numbers)
        index.insert_blocks(responses)

### WIP unfinished TODO - finish it up
def BB_updater_auto(startBlock=0, lastBlock=None, saveTXs=True, parseTXs=False):
    print(f"{t} BB_updater_auto")
    #get the block it should start from
    indexed_block = index.latest_block()
    #start_block = 0 if indexed_block is None else indexed_block
    #start_block = startBlock if indexed_block is None else indexed_block  # this works but starts from the last block saved - so I need to override this
    start_block = startBlock

    latest_block_number = web3.get_latest_block_number()
    end_block = lastBlock if lastBlock is not None else latest_block_number
    #override the latest_block_number just for dev purposes to check only a few at a time
    # end_block = start_block + chunk_size ## ------------ REMEMBER TO COMMENT THIS OUT
    print(f"\n{t} indexed_block: {indexed_block} - start_block: {start_block} - startBlock param: {startBlock} - end_block: {end_block}")

    n = end_block - start_block
    start_time = time.time()
    last_diff = 0
    update_rate = 60 # this prints to console the update once every 60 seconds or minutes?

    print(f'\n{t} syncing {n} blocks - from #{start_block} to #{end_block}')
    block_chunks = chunks(range(start_block, latest_block_number), chunk_size)
    for block_numbers in block_chunks:
        block_number = block_numbers[0]
        print(f"parse {block_number}")

        # calculate and display the missing time
        duration = time.time() - start_time
        diff = duration % update_rate
        #print(f"{block_number}  /  diff: {diff}  /  duration: {duration}")
        if diff < last_diff:
            remaining_blocks = end_block - block_number
            processed_blocks = block_number - start_block
            remaining_duration = (remaining_blocks / processed_blocks) * duration
            print(f'#{block_number} remaining {remaining_duration:.1f}s = {datetime.timedelta(seconds = remaining_duration)}')
        last_diff = diff

        ## first check if you already have the block in the DB and only query it if you don't
        ## TODO check if I'm really skipping the block because block_number is always equal to the block_numbers[0]
        if not index.is_block_in_db(block_number):
            # batch call the node to get the data and add it to the DB
            if saveTXs == False:
                responses = web3.batch_get_block_by_number(block_numbers)
                #print(f"{t} responses: {responses}")
                index.insert_blocks(responses)
            else: 
                responses_blocks, responses_txs =  web3.batch_get_blocks_and_txs(block_numbers, parseTXs)
                #print(f"{t} responses_blocks: {responses_blocks}")
                #print(f"\n\n{t} responses_txs: {responses_txs}")
                index.insert_blocks(responses_blocks)
                index.insert_transactions(responses_txs)
        else:
            print(f"{t}block {block_number} is already in the db")
            responses_blocks, responses_txs = [], []
            next(block_chunks)

        ## TODO - start from here. how to have feedback on the console if responses is empty? this creates problems
        #print(f"{t} finished saving {len(responses_blocks)} blocks ({responses_blocks[0]['id']}-{responses_blocks[-1]['id']} + {len(responses_txs)} transactions / in {time_delta(start_time)}") 

    #print(f"\n\n--------------------\n{t} finished saving everything {len(responses_blocks)} blocks ({start_block}-{end_block} + {len(responses_txs)} transactions / in {time_delta(start_time)}") 


## Used to extract some samples form the blockchain
def sample_extractor(fromN, batch_size, saveTXs=True, parseTXs=False):
    start_block = fromN
    end_block = start_block + batch_size
    print(f"{t} / sample_extractor extract {batch_size} blocks, starting from blockN {start_block}")
    block_numbers = []
    for block_number in range(start_block, end_block):
        block_numbers.append(block_number)

    start_time = time.time()
    print(f"{t} - block_numbers: {block_numbers} - start_time:{start_time}")    
    #print(f"{t} - range(start_block, end_block) = {range(start_block, end_block)}")
    if saveTXs == False:
        responses = web3.batch_get_block_by_number(block_numbers)
        print(f"{t} responses: {responses}")
        index.insert_blocks(responses)

    else:
        # save also the TXs
        responses_blocks, responses_txs =  web3.batch_get_blocks_and_txs(block_numbers, parseTXs)
        #print(f"{t} responses_blocks: {responses_blocks}")
        #print(f"\n\n{t} responses_txs: {responses_txs}")
        index.insert_blocks(responses_blocks)
        index.insert_transactions(responses_txs)
        
        print(f"{t} finished saving {len(responses_blocks)} blocks ({start_block}-{end_block} + {len(responses_txs)} transactions / in {time_delta(start_time)}")
       
        
def time_delta(start_time):
    tdelta = time.time() - start_time
    # TODO convert time difference to hours and seconds
    return tdelta


# BATCH GET ALL BLOCKS
start_block = 15535703#15418980 # restarted here because it got blocked
#15342781 # date(2022,8,15)  Block
#9193266   #start from 2020-01-01 - interruputed at block 9793360 (2020-04-02)
lastBlock = 15535901#15537393 # this is the block of the merge
chunk_size = ScrapeConfig.blocks_batch_size
BB_updater_auto(start_block, lastBlock, ScrapeConfig.save_txs, ScrapeConfig.query_tx_info )



''' SAMPLE EXTRACTOR works
batch_size = 100
start_block_num = 13000000
sample_extractor(start_block_num, batch_size)
# 12000000 # of 2021-03-08
# 11000000 # of 2020-10-06
#10842363 # of 2020-09-11
# 9842363 of 2020-04-10

# 9193266 First Block of 2020
#'''




"""
indexed_block = index.latest_block()
start_block = 0 if indexed_block is None else indexed_block
n = end_block - start_block
start_time = time.time()
last_diff = 0
update_rate = 60

print(f'syncing #{start_block} to #{end_block}')
for block_numbers in chunks(range(start_block, latest_block_number), chunk_size):
    block_number = block_numbers[0]
    duration = time.time() - start_time
    diff = duration % update_rate
    if diff < last_diff:
        remaining_blocks = end_block - block_number
        processed_blocks = block_number - start_block
        remaining_duration = (remaining_blocks / processed_blocks) * duration
        print(f'#{block_number} {remaining_duration:.1f}s')
    last_diff = diff

    responses = web3.batch_get_block_by_number(block_numbers)
    index.insert_blocks(responses)
#"""