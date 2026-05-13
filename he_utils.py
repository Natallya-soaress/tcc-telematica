import tenseal as ts
import time

context = ts.context(
    ts.SCHEME_TYPE.CKKS,
    poly_modulus_degree=4096,
    coeff_mod_bit_sizes=[40, 20, 40]
)

context.global_scale = 2**20
context.generate_galois_keys()


def encrypt_vector(data):
    start = time.perf_counter()
    enc = ts.ckks_vector(context, data)
    end = time.perf_counter()
    return enc, end - start


def decrypt_vector(enc):
    start = time.perf_counter()
    dec = enc.decrypt()
    end = time.perf_counter()
    return dec, end - start