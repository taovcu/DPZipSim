import argparse
import os, sys
from time import gmtime, strftime
import timeit
import time
import gzip
import zstd
import lz4
import lz4.frame
import numpy as np

compressed_size = []
chunk_kb = 0

def getfilesize(filename):
    try:
        ret = os.path.getsize(filename)
    except:
        print("except in getfilesize {}".format(filename))
        sys.exit(1)
    return ret

def chunk_file_in_bytes(filename, chunk_kB):
    ret = []

    # Open original file in read only mode
    if not os.path.isfile(filename):
        print("No such file as: \"%s\"" % filename)
        return

    filesize=getfilesize(filename)
    with open(filename,"rb") as fr:
        file_bytes = fr.read()
        if not chunk_kB:
            return [file_bytes]
        splitsize = chunk_kB * 1024
        n_splits = (filesize + splitsize - 1)//splitsize
        return [file_bytes[i * splitsize:(i + 1) * splitsize] for i in range(n_splits)] 

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
        return int(s * (1.33+1.21) / 2)
    if s <= 738:
        return int(s * (1.21+1.21) / 2)
    if s <= 1202:
        return int(s * (1.21+1.05) / 2)
    if s <= 1624:
        return int(s * (1.05+1.03) / 2)
    if s <= 1734:
        return int(s * (1.03+1.03) / 2)
    if s <= 1883:
        return int(s * (1.00+1.03) / 2)
    if s <= 2048:
        return int(s * (1.02+1.00) / 2)
    if s <= 2539:
        return int(s * (1.04+1.02) / 2)
    if s <= 3151:
        return int(s * (1.00+1.04) / 2)

    return s


def compress_bytes(compressor, direction, cmp_level, byte_content):
    start_t = time.time()
    end_t = time.time()

    start_t = time.time()
    if compressor == 'gzip':
        if not direction:
            if cmp_level:
                ret = gzip.compress(byte_content, compresslevel=cmp_level)
            else:
                ret = gzip.compress(byte_content, compresslevel=6)
            compressed_size.append(len(ret))

        if direction == 1:
            ret = gzip.decompress(byte_content)

    if compressor in ['zstd', 'dpzip']:
        if not direction:
            if cmp_level:
                ret = zstd.compress(byte_content, cmp_level, 1)
            else:
                ret = zstd.compress(byte_content, 3, 1)

            if compressor == 'zstd':
                compressed_size.append(len(ret))
            if compressor == 'dpzip':
                compressed_size.append(zstd2dpzip_size_ratio(len(ret)))

        if direction == 1:
            ret = zstd.decompress(byte_content)

    if compressor == 'lz4':
        if not direction:
            ret = lz4.frame.compress(byte_content)
        if direction == 1:
            ret = lz4.frame.decompress(byte_content)
        compressed_size.append(len(ret))

    end_t = time.time()
    comp_time = end_t - start_t

    return ret, len(ret)/len(byte_content), len(byte_content)/1024/1024/(comp_time), 0


def print_compress_metrics(compressed_size_list):
    print("Total compressed bytes: {}".format(sum(compressed_size_list)))
    print("Percetile compressed file size:\t1\t10\t20\t30\t40\t50\t60\t70\t80\t90\t95\t99\t99.9")
    print("                               \t{:.0f}\t{:.0f}\t{:.0f}\t{:.0f}\t{:.0f}\t{:.0f}\t{:.0f}\t{:.0f}\t{:.0f}\t{:.0f}\t{:.0f}\t{:.0f}\t{:.0f}".format(np.percentile(compressed_size_list, 1), np.percentile(compressed_size_list, 10),
            np.percentile(compressed_size_list, 20), np.percentile(compressed_size_list, 30), np.percentile(compressed_size_list, 40), np.percentile(compressed_size_list, 50),
            np.percentile(compressed_size_list, 60), np.percentile(compressed_size_list, 70), np.percentile(compressed_size_list, 80), np.percentile(compressed_size_list, 90),
            np.percentile(compressed_size_list, 95), np.percentile(compressed_size_list, 99), np.percentile(compressed_size_list, 99.9)
        ))
    print("Average compression ratio: {:.2f}".format(len(compressed_size_list)*chunk_kb*1024 / sum(compressed_size_list)))


def compress_in_mem_chunks(compressor, direction, cmp_level, f_name, chunk_kB, hw_granularity, m_distance):
    chunk_cmp_ratio_list = []
    chunk_cmp_time_list = []
    compressed_chunk_size_list = []
    input_num_bytes = 0
    output_num_bytes = 0
    wasted_num_bytes = 0

    # if f_name is smaller than 1GB
    if getfilesize(f_name) <= 1024 * 1024 * 1024:
        byte_chunk_list = chunk_file_in_bytes(f_name, chunk_kB)
        for f in byte_chunk_list:
            output, ratio, speed, ent = compress_bytes(compressor, direction, cmp_level, f)
            input_num_bytes += len(f)
            output_num_bytes += len(output)
            if hw_granularity and len(output) % hw_granularity:
                wasted_num_bytes += (hw_granularity - len(output) % hw_granularity)
            chunk_cmp_time_list.append(speed)
            chunk_cmp_ratio_list.append(ratio)
            compressed_chunk_size_list.append(len(output))
    else:
        with open(f_name, 'rb') as f:
            for piece in read_in_chunks(f):
                byte_chunk_list = chunk_mem_in_bytes(piece, chunk_kB)
                for f in byte_chunk_list:
                    output, ratio, speed, ent = compress_bytes(compressor, direction, cmp_level, f)
                    chunk_cmp_time_list.append(speed)
                    chunk_cmp_ratio_list.append(ratio)

    # compress
    if not direction:
        print_compress_metrics(compressed_size)

def do_compress():
    global chunk_kb
    parser = argparse.ArgumentParser()
    parser.add_argument("compressor", type=str, choices=['gzip', 'zstd', 'lz4', 'dpzip'],
                        help="choose the compressor")
    parser.add_argument("input_file_path", type=str,
                        help="the input file path must be specified")
    args = parser.parse_args()
    answer = args.input_file_path
    chunk_kb = 4

    print("Original dataset bytes: {}".format(getfilesize(args.input_file_path)))
    compress_in_mem_chunks(args.compressor, 0, 1, args.input_file_path, 4, 0, 0)


if __name__ == '__main__':
    do_compress()

