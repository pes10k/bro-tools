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
    return [(v, os.path.join(workpath, os.path.basename(k))) for k, v in files_to_combine.items()]

def merge(files, dest_path):
    # If the file has already been generated, don't generate it again
    if os.path.isfile(dest_path) and os.path.getsize(dest_path):
        return True

    read_headers_from_any_file = False
    headers = ""
    lines = []
    for compressed_file in files:
        with gzip.open(compressed_file, 'r') as source_h:
            read_headers_from_this_file = False
            for line in source_h:
                if line[0] == "#":
                    if not read_headers_from_any_file:
                        headers += line
                        read_headers_from_this_file = True
                else:
                    lines.append(line)
            if read_headers_from_this_file:
                read_headers_from_any_file = True

    if len(headers) == 0 or len(lines) == 0:
        return False

    # Now sort all the rows.  This will be big
    lines.sort()
    dest_h = open(dest_path, 'w')
    dest_h.write(headers)
    for line in lines:
        dest_h.write(line)
    dest_h.close()
    return True

if __name__ == "__main__":
    """If we're running as a script directly, read in a list of file names
    from stdin and attempt to merge those together into the given argument
    directory"""
    import argparse
    import sys

    parser = argparse.ArgumentParser(description='Merge bro records together into a given destination directory. File names to merge are read in from STDIN.')
    parser.add_argument('--dest', '-d', default=".",
                        help="Path to a directory to write merged files to on disk. Defaults to the current directory")
    parser.add_argument('--verbose', '-v', action="store_true",
                        help="If set, lots of information about the merging process will be printed out.")
    parser.add_argument('--test', '-t', action="store_true",
                        help="If set, no files will be written to disk.  Possibly useful in combination with --verbose.")
    args = parser.parse_args()

    def info(msg):
        if args.verbose:
            print msg

    input_files = sys.stdin.read().strip().split("\n")
    info("Received {0} files to merge".format(len(input_files)))

    grouped_files = group_records(input_files, args.dest)
    info("Will merge files as follows:\n")

    for orig_files, dest_file in grouped_files:
        info(" - {0} will contain:".format(dest_file))
        for orig_file in orig_files:
            info("\t - {0}".format(orig_file))

        if not args.test:
            merge(orig_files, dest_file)
