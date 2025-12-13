from dotenv import load_dotenv
import os
from typing import Dict

class S3:

    def __init__(self, bc_api_key:str=None, aws_region:str=None, s3_bucket_name:str=None):
       
        self.aws_region = aws_region if aws_region else self.get_s3_bucket_info()["aws_region"]
        self.s3_bucket_name = s3_bucket_name if s3_bucket_name else self.get_s3_bucket_info()["s3_bucket_name"]
    
    def get_s3_bucket_info(self) -> Dict:
        load_dotenv()
        s3_info = {
            "aws_region": os.environ.get("AWS_REGION"),
            "s3_bucket_name": os.environ.get("S3_BUCKET_NAME")
        }
        if not s3_info["aws_region"] or not s3_info["s3_bucket_name"]:
            raise ValueError("AWS_REGION or S3_BUCKET_NAME not found in environment variables. Please store them in a .env file in the project root.")
        return s3_info
