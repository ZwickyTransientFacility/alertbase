from alertbase.alert import AlertRecord
from alertbase.alert_tar import iterate_tarfile
import timeit
import numpy as np


def main():
    do_benchmark("read_precompile")
    do_benchmark("read_fastavro_safe")
    do_benchmark("read_safe")
    do_benchmark("read_subschema")
    do_benchmark("read_fastavro_manual_decoder")
    do_benchmark("read_fastavro_subschema")


def do_benchmark(funcname):
    n = 1
    results = np.array(
        timeit.repeat(
            f"{funcname}()",
            number=n,
            repeat=3,
            setup=f"from __main__ import {funcname}; gc.enable()",
        ),
    )
    results /= n
    best_estimate = results.min()
    mb_per_sec = untarred_size / best_estimate / 1e6

    results /= n_alerts
    results *= 1000 # up to ms
    print(f"{results.min():.3f} | {mb_per_sec:.2f} |")



filepath = "testdata/alertfiles/ztf_public_20210120.tar.gz"
untarred_size = 170277376  # bytes
n_alerts = 2567

def read_precompile():
    iterator = iterate_tarfile(filepath, AlertRecord.from_file_precompile)
    return sum(1 for _ in iterator)

def read_safe():
    iterator = iterate_tarfile(filepath, AlertRecord.from_file_safe)
    return sum(1 for _ in iterator)

def read_subschema():
    iterator = iterate_tarfile(filepath, AlertRecord.from_file_subschema)
    return sum(1 for _ in iterator)

def read_fastavro_safe():
    iterator = iterate_tarfile(filepath, AlertRecord.from_file_fastavro_safe)
    return sum(1 for _ in iterator)

def read_fastavro_subschema():
    iterator = iterate_tarfile(filepath, AlertRecord.from_file_fastavro_subschema)
    return sum(1 for _ in iterator)

def read_fastavro_manual_decoder():
    iterator = iterate_tarfile(filepath, AlertRecord.from_file_fastavro_unsafe)
    return sum(1 for _ in iterator)

if __name__ == "__main__":
    main()
