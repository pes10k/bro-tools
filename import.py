from connection import db, bro_records

import sys

for record in bro_records(sys.stdin):
  print record
