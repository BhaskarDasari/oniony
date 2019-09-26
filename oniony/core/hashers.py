import hashlib

import binascii


def pbkdf2(string, salt, iterations=10, hash_name='sha256', encoding='utf-8'):
    """A PBKDF2 string hasher.

    This hasher uses `SHA-256` as the underlying algorithm with a customizable number of `iterations`.

    :param string: String to be hashed
    :param salt: Salt to be added to string
    :param iterations: Number of iterations
    :param hash_name: Underlying hashing algorithm (default `SHA-256`)
    :param encoding: The `string` encoding, defaults to `'utf-8'`
    :return: A hashed string

    """
    dk = hashlib.pbkdf2_hmac(hash_name=hash_name,
                             password=string.encode(encoding),
                             salt=salt.encode(),
                             iterations=iterations)
    return binascii.hexlify(dk)
