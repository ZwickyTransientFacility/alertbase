from alertbase.alert import AlertRecord
import timeit
import numpy as np


def main():
    do_benchmark("read_safe")
    do_benchmark("read_manual_decoder")
    do_benchmark("read_subschema")
    do_benchmark("read_fastavro_safe")
    do_benchmark("read_fastavro_subschema")


def do_benchmark(funcname):
    print(f"benchmarking {funcname}")
    n = 200
    results = np.array(
        timeit.repeat(
            f"{funcname}()",
            number=n,
            repeat=10,
            setup=f"from __main__ import {funcname}; gc.enable()",
        ),
    )
    results /= n
    results *= 1000 # convert up to milliseconds
    print(f"min: {results.min():.3f}ms  mean: {results.mean():.3f}ms  std: {results.std():.3f}ms")



filepath = "testdata/alertfiles/1311156250015010003.avro"

def read_manual_decoder():
    with open(filepath, 'rb') as f:
        AlertRecord.from_file_unsafe(f)

def read_safe():
    with open(filepath, 'rb') as f:
        AlertRecord.from_file_safe(f)

def read_subschema():
    with open(filepath, 'rb') as f:
        AlertRecord.from_file_subschema(f)

def read_fastavro_safe():
    with open(filepath, 'rb') as f:
        AlertRecord.from_file_fastavro_safe(f)

def read_fastavro_subschema():
    with open(filepath, 'rb') as f:
        AlertRecord.from_file_fastavro_subschema(f)

if __name__ == "__main__":
    main()
