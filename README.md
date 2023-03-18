# DPZipSim

DPZipSim is a tool to simulate the compression ratio of DPZip hardware compressor on a user specified dataset. 
DPZipSim employs Zstd to conduct compression on the candidate dataset and adjust the size of compressed unit according to the corresponding relationship 
between Zstd and DPZip to accurately simulate the compression ratio of the hardware compressor. 
Our evaluation with silesia dataset shows that DPZipSim can deliver a compression ratio simulation accuracy of 98.7%. 

# Execution

$python3 dpzip\_sim.py dpzip\_sim dataset.bin

Note: User can replace the dpzip\_sim parameter to zstd, gzip, lz4 for comparison

# Sample Output

Original dataset bytes: 1000949760

Total compressed bytes: 762469483

Percetile compressed unit size (Original unit is fixed 4KB):	

  |1	|10	|20	|30	|40	|50	|60	|70	|80	|90	|95	|99	|99.9|
  |----|---|---|-----|---|----|---|---|-----|---|---|-----|---|
  |1606.0	|1935.0	|2027.0	|2188.0	|3458.0	|3574.0	|3640.0	|3685.0	|3829.0	|4028.0	|4106.0	|4106.0	|4106.0|
                                
Average compression ratio: 1.31
