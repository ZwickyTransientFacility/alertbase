from alertbase.alert import AlertRecord
import timeit
import numpy as np


def main():
    do_benchmark("precompile")
    do_benchmark("safe")
    do_benchmark("manual_decoder")
    do_benchmark("subschema")
    do_benchmark("fastavro_safe")
    do_benchmark("fastavro_manual_decoder")
    do_benchmark("fastavro_subschema")


def do_benchmark(funcname):
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
    print(f"{funcname} | {results.min():.3f}")



filepath = "testdata/alertfiles/1311156250015010003.avro"

def manual_decoder():
    with open(filepath, 'rb') as f:
        AlertRecord.from_file_unsafe(f)

def safe():
    with open(filepath, 'rb') as f:
        AlertRecord.from_file_safe(f)

def subschema():
    with open(filepath, 'rb') as f:
        AlertRecord.from_file_subschema(f)

def fastavro_safe():
    with open(filepath, 'rb') as f:
        AlertRecord.from_file_fastavro_safe(f)

def fastavro_subschema():
    with open(filepath, 'rb') as f:
        AlertRecord.from_file_fastavro_subschema(f)

def fastavro_manual_decoder():
    with open(filepath, 'rb') as f:
        AlertRecord.from_file_fastavro_unsafe(f)

def precompile():
    with open(filepath, 'rb') as f:
        AlertRecord.from_file_precompile(f)


if __name__ == "__main__":
    main()
