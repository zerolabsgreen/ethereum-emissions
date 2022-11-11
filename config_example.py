# rename this file to just config.py

class RPCconfig:
    rpc_location = "LOCAL"#"REMOTE"# #
    rpc_path_remote = "https://eth-mainnet.g.alchemy.com/v2/YOUR_API_KEY_HERE"
    rpc_path_local = "~/.ethereum/geth.ipc"
    
class ScrapeConfig:    
    blocks_batch_size = 100     # max blocks to request the node at a time - for dev purposes
    save_txs = True             # allows to save the TX info found inside the block, sometimes it's nothing
    query_tx_info = False       # query the node if the block info doesn't have the TX - this can take a long time and resources so you can disable this

class Day1Config:
    start_block = 15531512
    end_block = 15537393