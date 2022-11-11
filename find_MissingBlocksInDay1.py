
import sqlite3

from block_index_remote import Block

##-------- CONFIG
db_file ='block_index.sqlite3'
db_loc = '/Users/b/Documents/01_Works/20211201_Zero-v2/04_Product/03_Code/03_BlockGarden/00_EthereumEmissions/cache/'+db_file
flags = '?mode=ro'

table_name = "day1"
query = f"SELECT * FROM {table_name}"

##-------CONNECT 
db = sqlite3.connect(f'file:{db_loc}{flags}', uri=True)
dbresults = db.cursor().execute(query)

#print(dbresults.fetchone())
#block = Block(*dbresults.fetchone()) #the * expands the row into all the parameters
#print(block)

# --- PARSE
missing_blocks = []
parsed_blocks = []
## TODO maybe turn it into a for loop so you can see which blocks are missing
for row in dbresults:
    block = Block(*row)
    #print(block)
    block_number = row[0]
    if (len(parsed_blocks) == 0):
        parsed_blocks.append(block_number)
    else:
        previous = parsed_blocks[-1]
        if ( block_number != previous + 1):
            #here is a missing number mark it
            missing_blocks.append(block_number)
            print(f"found missing block between {previous} and {block_number}: {block_number-previous} missing blocks")
        parsed_blocks.append(block_number)

print(f"{len(missing_blocks)} missing blocks / {len(parsed_blocks)} parsed blocks")

#### found missing block between 15535702 and 15535902: 200 missing blocks
## So download from block 15535703  to block 15535901 both included