
import datetime
from collections import defaultdict

#from block_index import BlockIndex # Kyles original 
from block_index_remote import BlockIndex ##BB
from block_classifier import BlockClassifier

from tqdm import tqdm
import pandas as pd
import numpy as np

from config import Day1Config as d1

index = BlockIndex(read_only=True, table='day1')
classifier = BlockClassifier()

block_labels = defaultdict(lambda: defaultdict(int)) #Kyle's way to build the dictionary
blocks_with_locations = [] 

csv = "block_number, location, extra_data\n"

#total = index.latest_block() - 1
#for block in tqdm(index.list_blocks(skip_genesis=True), total=total):
blocks_to_parse = index.list_blocks_range(d1.start_block, d1.end_block, "day1")
total = d1.end_block - d1.start_block +2 ## includes both extremes
print(f" total blocks_to_parse: {total}")

for block in tqdm(blocks_to_parse, total=total):
    #date = block.get_datetime().date()
    block_number = block.get_block_number()

    # first, try to label the block based on the extra data
    label = classifier.classify_extra_data(block.extra_data)
    if label is not None:
        label = 'extraData:' + label
    else:
        # if that doesn't work, label it based on the pool
        label = classifier.classify_miner(block.miner)
        if label is not None:
            label = 'pool:' + label
    # if neither work, call it 'unknown'
    if label is None:
        label = 'unknown'
    
    # kyle record it based on date or block_number
    #block_labels[date][label] += 1

    # this shows for each block
    block_labels[block_number][label] += 1  #works

    '''## BB 
    blocks_with_locations.append( {
        'block_number': block_number,
        'extra_data': block.extra_data_decoded(),
        'location': label,
    })
    ##'''
    ## this causes problems
    #blocks_with_locations[block_number]['location'] = label
    #blocks_with_locations[block_number]['extra_data'] = block.extra_data_decoded()

    csv += f"{block_number}, {label} \n"

##this is how Kyle does it but it outputs a colum for each 
'''
df = pd.DataFrame(block_labels).T.sort_index()
df.index.name = 'block_number'
df.to_csv('output/block-labels.csv', float_format='%.0f')
##'''

#df2 = pd.DataFrame(blocks_with_locations).T.sort_index()
#df2.index.name = 'block_number'
#df2.to_csv('output/block-locations.csv', float_format='%.0f')

from io import StringIO
csvStringIO = StringIO(csv) #convert String to StringIO
df2 = pd.read_csv(csvStringIO, sep=",")
df2.index.drop()
df2.to_csv('output/block-locations.csv', float_format='%.0f')







