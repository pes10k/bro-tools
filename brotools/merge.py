"""Iterators and functions for merging collections of bro records spread
across multiple files_to_combine"""

import gzip
import os

def merged_bro_records(files, work_dir="/tmp"):
    """Returns an iterator of path names to gziped combined bro data records,
    merged and sorted together.

    Args:
        path -- a path, as a string, to a directory of compressed, divided
                bro archive files

    Keyword Args:
        work_dir -- A path to write the temporary files to.  Note that these
                    files can be safely deleted after they're used
                    by a caller with os.remove(a_path)

    Return:
        A generator that returns path names to compressed combined gzip data
    """
    files_to_combine = {}

    for f in files:
        combined_file_name = f[:-5]
        if combined_file_name not in files_to_combine:
            files_to_combine[combined_file_name] = []
        files_to_combine[combined_file_name].append(f)

    for file_name in files_to_combine:
        tmp_name = os.path.join(work_dir, os.path.basename(file_name) + ".gz")

        # If the file has already been generated, don't generate it again
        if os.isfile(tmp_name) and os.path.getsize(tmp_name):
            yield tmp_name
            continue

        read_headers = False
        headers = ""
        lines = []
        for compressed_file in files_to_combine[file_name]:
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
        dest_h = gzip.open(tmp_name, 'wb')
        dest_h.write(headers)
        for line in lines:
            dest_h.write(line)
        dest_h.close()
        yield tmp_name
