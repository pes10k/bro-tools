"""Iterators and functions for merging collections of bro records spread
across multiple files_to_combine"""

import gzip
import os

def group_records(files, workpath="/tmp"):
    files_to_combine = {}
    for f in files:
        combined_file_name = f[:-5]
        if combined_file_name not in files_to_combine:
            files_to_combine[combined_file_name] = []
        files_to_combine[combined_file_name].append(f)
    return [(v, os.path.join(workpath, os.path.basename(k) + ".gz")) for k, v in files_to_combine.items()]

def merge(files, dest_path):

    # If the file has already been generated, don't generate it again
    if os.path.isfile(dest_path) and os.path.getsize(dest_path):
        return dest_path

    read_headers = False
    headers = ""
    lines = []
    for compressed_file in files:
        with gzip.open(compressed_file, 'r') as source_h:
            for line in source_h:
                if line[0] == "#":
                    if not read_headers:
                        headers += line
                else:
                    lines.append(line)
            read_headers = True

    # Now sort all the rows.  This will be big
    lines.sort()
    dest_h = gzip.open(dest_path, 'wb')
    dest_h.write(headers)
    for line in lines:
        dest_h.write(line)
    dest_h.close()
    return dest_path
