'''
Fetch latest version of the db from S3 and save it locally. 
'''
from impulse.collection.database import ImpulseDB
from impulse.collection.s3_manager import S3Manager

local_db_path = './impulse.db'

s3 = S3Manager()
db = ImpulseDB(
    db_path = local_db_path, 
    s3_manager = s3
    )

result = db.pull()
print(f"Pull result: {result}")
