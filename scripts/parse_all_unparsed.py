"""
Script to parse all unparsed replay files in a given directory using a predefined parsing pipeline. Checks db for already parsed files to avoid duplication.
Saves parsed files to the specified output directory.
"""

from impulse.parsing import ParsingPipeline, ReplayParser
from impulse.collection.database import ImpulseDB

parser = ReplayParser.from_preset('standard', fps=30.0)
db = ImpulseDB()
pipeline = ParsingPipeline(parser, db)

raw_replays_dir = './replays/raw'
output_dir = './replays/parsed'

parse_result = pipeline.parse_unparsed(raw_replays_dir, output_dir)

