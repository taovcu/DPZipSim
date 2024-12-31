import argparse
import os
import sys
import time
import gzip
import zstd
import lz4.frame

import numpy as np

compressed_size = []
chunk_kb = 0

def getfilesize(filename):
    try:
        ret = os.path.getsize(filename)
    except Exception:
        print(f"Exception in getfilesize: {filename}")
        sys.exit(1)
    return ret

def chunk_file_in_bytes(filename, chunk_kB):
    if not os.path.isfile(filename):
        print(f"No such file: \"{filename}\"")
        return []

    filesize = getfilesize(filename)
    with open(filename, "rb") as fr:
        file_bytes = fr.read()
        if not chunk_kB:
            return [file_bytes]
        splitsize = chunk_kB * 1024
        return [file_bytes[i * splitsize:(i + 1) * splitsize] for i in range((filesize + splitsize - 1) // splitsize)] 

def chunk_mem_in_bytes(data_content, chunk_kB):
    ret = []
    datasize=len(data_content)
    if not chunk_kB:
        return [data_content]
    splitsize = chunk_kB * 1024
    n_splits = (datasize + splitsize - 1)//splitsize
    return [data_content[i * splitsize:(i + 1) * splitsize] for i in range(n_splits)]

def zstd2dpzip_size_ratio(s):
    if s <= 380:
        return int(s * 1.33)
    if s <= 633:
        return int(s * (1.33 + 1.21) / 2)
    if s <= 738:
        return int(s * (1.21 + 1.21) / 2)
    if s <= 1202:
        return int(s * (1.21 + 1.05) / 2)
    if s <= 1624:
        return int(s * (1.05 + 1.03) / 2)
    if s <= 1734:
        return int(s * (1.03 + 1.03) / 2)
    if s <= 1883:
        return int(s * (1.00 + 1.03) / 2)
    if s <= 2048:
        return int(s * (1.02 + 1.00) / 2)
    if s <= 2539:
        return int(s * (1.04 + 1.02) / 2)
    if s <= 3151:
        return int(s * (1.00 + 1.04) / 2)
    return s

def compress_bytes(compressor, direction, cmp_level, byte_content):
    start_t = time.time()

    if compressor == 'gzip':
        if not direction:
            level = cmp_level if cmp_level else 6
            ret = gzip.compress(byte_content, compresslevel=level)
            compressed_size.append(len(ret))
        else:
            ret = gzip.decompress(byte_content)

    elif compressor == 'snappy':
        if not direction:
            ret = snappy.compress(byte_content)
            compressed_size.append(len(ret))
        else:
            ret = snappy.uncompress(byte_content)


    elif compressor in ['zstd', 'dpzip']:
        if not direction:
            level = cmp_level if cmp_level else 3
            ret = zstd.compress(byte_content, level, 1)
            if compressor == 'zstd':
                compressed_size.append(len(ret))
            elif compressor == 'dpzip':
                compressed_size.append(zstd2dpzip_size_ratio(len(ret)))
        else:
            ret = zstd.decompress(byte_content)

    elif compressor == 'lz4':
        if not direction:
            ret = lz4.frame.compress(byte_content)
            compressed_size.append(len(ret))
        else:
            ret = lz4.frame.decompress(byte_content)

    comp_time = time.time() - start_t
    compression_ratio = len(ret) / len(byte_content)
    return ret, compression_ratio, len(byte_content) / (1024 * 1024) / comp_time

def print_compress_metrics(compressed_size_list):
    print(f"Total compressed bytes: {sum(compressed_size_list)}")
    percentiles = [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99]
    percentile_values = ",\t".join([f"{np.percentile(compressed_size_list, p):.0f}" for p in percentiles])
    print(f"Percentile compressed file size:\n\t{percentile_values}")
    average_ratio = (len(compressed_size_list) * chunk_kb * 1024) / sum(compressed_size_list)
    print(f"Average compression ratio: {average_ratio:.2f}")

def compress_in_mem_chunks(compressor, direction, cmp_level, f_name, chunk_kB):
    if getfilesize(f_name) <= 1024 * 1024 * 1024:
        byte_chunk_list = chunk_file_in_bytes(f_name, chunk_kB)
        for chunk in byte_chunk_list:
            _, _, _ = compress_bytes(compressor, direction, cmp_level, chunk)
    else:
        with open(f_name, 'rb') as f:
            for piece in read_in_chunks(f):
                byte_chunk_list = chunk_mem_in_bytes(piece, chunk_kB)
                for chunk in byte_chunk_list:
                    _, _, _ = compress_bytes(compressor, direction, cmp_level, chunk)

    if not direction:
        print_compress_metrics(compressed_size)

def read_in_chunks(file_object, chunk_size=1024*1024*1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1GB."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data

def do_compress():
    global chunk_kb
    parser = argparse.ArgumentParser(description="Compress files using various compressors.")
    parser.add_argument("compressor", type=str, choices=['gzip', 'zstd', 'lz4', 'dpzip'],
                        help="Choose the compressor: gzip, zstd, lz4, dpzip")
    parser.add_argument("input_file_path", type=str,
                        help="The input file path must be specified")
    args = parser.parse_args()
    chunk_kb = 4

    input_size = getfilesize(args.input_file_path)
    print(f"Original dataset bytes: {input_size}")
    compress_in_mem_chunks(args.compressor, direction=0, cmp_level=1, 
                           f_name=args.input_file_path, chunk_kB=chunk_kb)

if __name__ == '__main__':
    do_compress()

