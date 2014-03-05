from .connection import db
import sys

with open(sys.stdin, 'r') as stdin:
  for row in stdin.xreadline():
    fields = row.split("\t")
    print fields
