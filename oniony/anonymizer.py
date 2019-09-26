# coding=utf-8
from array import array
from functools import reduce

import ipaddress
from Crypto.Cipher import AES
from netaddr import IPNetwork

# Prefix-preserving IP anonymization
from oniony.core.hashers import pbkdf2

BITS = 128
_mask = reduce(lambda x, y: (x << 1) | y, [1] * BITS)
MASK = [_mask >> i for i in range(BITS)]


class IPAnonymization:

    def __init__(self):
        pass

    @staticmethod
    def _createCipher(key):
        return AES.new(key, AES.MODE_ECB)

    """
    Create a byte array from the cipher string

    :param cipher: The Cryptho cipher (AES)
    :param key: The crypto key (string)
    :returns The encrypted string
    """

    @staticmethod
    def _createPadding(cipher, key):
        padding = array('B')
        padding.fromstring(cipher.encrypt(key))
        result = 1
        for x in padding:
            result = (result << 8) | x
        return result

    @staticmethod
    def _unpack(int_value, int_value_len):
        return array('B', [(int_value >> (i * 8)) & 0xff for i in reversed(range(int_value_len))])

    @staticmethod
    # assuming IPv4
    def _anonymize(address, padding, cipher):
        ai = address << 96

        def shift(a, index):
            return a >> (BITS - index) << (BITS - index)

        def pad_mask(s, index):
            return s | (padding & MASK[index])

        def enc_p(p):
            return cipher.encrypt(IPAnonymization._unpack(p, 16).tostring())

        def rshift(e):
            return bytearray(e)[0] >> 7

        return address ^ reduce(lambda x, y: (x << 1) | y,
                                [rshift(enc_p(pad_mask(shift(ai, i), i))) for i in range(32)])

    @staticmethod
    def anonymize(address, aes_key, padding_key):
        cipher = IPAnonymization._createCipher(aes_key)
        padding = IPAnonymization._createPadding(cipher, padding_key)
        ip = IPNetwork(address)
        anon = IPAnonymization._anonymize(ip.value, padding, cipher)
        return '%d.%d.%d.%d' % (anon >> 24, (anon >> 16) & 0xff, (anon >> 8) & 0xff, anon & 0xff)

    @staticmethod
    def anonymize_list(addresses, aes_key, padding_key):

        if addresses:

            result = []

            for address in addresses:

                try:
                    ipaddress.ip_address(address)
                    result.append(IPAnonymization.anonymize(address=address,
                                                            aes_key=aes_key,
                                                            padding_key=padding_key))

                except ValueError:
                    result.append(None)

            return result
        else:
            return None


class HostnameAnonymization:
    def __init__(self):
        pass

    @staticmethod
    def anonymize(hostname, host_salt, domain_salt, iterations=10):

        if hostname is None:
            return None
        else:
            host_parts = hostname.split(".")

            if len(host_parts) > 1:
                host_hash = pbkdf2(host_parts[0], host_salt, iterations)
                domain_hash = pbkdf2(".".join(host_parts[1:]), domain_salt, iterations)
                return "{}.{}".format(host_hash, domain_hash)
            else:
                return pbkdf2(hostname, host_salt, iterations)

    @staticmethod
    def anonymize_list(hostnames, host_salt, domain_salt, iterations=10):

        if hostnames:

            return [

                HostnameAnonymization.anonymize(hostname=hostname,
                                                host_salt=host_salt,
                                                domain_salt=domain_salt,
                                                iterations=iterations)

                for hostname in hostnames
            ]

        else:

            return None
