from pyspark.sql.functions import udf, struct
from pyspark.sql.types import StringType

import oniony.core.hashers as hashers
from oniony.anonymizer import HostnameAnonymization


def hostname(column, host_salt, domain_salt, iterations):
    """Domain preserving anonymization of a hostname.
    This method uses the PBKDF2 hasher.

    :param column: The dataframe column to which apply the transformation
    :param host_salt: Salt to use for the host hashing
    :param domain_salt: Salt to use for the domain hashing
    :param iterations: Number of hash iterations
    :return: A domain preserving hashed hostname

    :Example:

        ``dataframe = dataframe.withColumn('hostname', hostname(dataframe.hostname, HOST_SALT, DOMAIN_SALT, 10)``
    """
    _udf = udf(lambda x: HostnameAnonymization.anonymize(hostname=x,
                                                         host_salt=host_salt,
                                                         domain_salt=domain_salt,
                                                         iterations=iterations))
    return _udf(column)


def pbkdf2(column, salt, iterations, default='', exclude=list()):
    """
    Returns a UDF ready to apply PBKDF2 to a dataframe column, by providing the column's name and parameters.

    :param column: Dataframe column to hash
    :param salt: Salt to use for hashing
    :param iterations: Number of iterations
    :param default: Default hash value if hash can't be applied (e.g. `None` or empty string)
    :param exclude: Values to bypass hashing (hash all by default)
    :return: A PBKDF2 hashed dataframe column

    .. code-block:: python

        dataframe = dataframe.withColumn('host', anonymization.pbkdf2(column=dataframe.host,
                                                                      salt='...',
                                                                      iterations=100))

    """

    def process(x, _salt, _iterations, _default, _exclude):

        if not x:  # not a valid value
            return _default
        else:
            if x in _exclude:  # exclude from hashing
                return x
            else:  # hash
                return hashers.pbkdf2(string=x,
                                      salt=_salt,
                                      iterations=_iterations)

    _udf = udf(lambda x: process(x, salt, iterations, default, exclude))
    return _udf(column)


def hostnameMapper(columns, iterations):
    """Domain preserving anonymization of a hostname.
    This method uses the PBKDF2 hasher.

    :param column: The dataframe column to which apply the transformation
    :param host_salt: Salt to use for the host hashing
    :param domain_salt: Salt to use for the domain hashing
    :param iterations: Number of hash iterations
    :return: A domain preserving hashed hostname

    :Example:

        ``dataframe = dataframe.withColumn('hostname', hostname(dataframe.hostname, HOST_SALT, DOMAIN_SALT, 10)``
    """

    def process(_columns):
        return HostnameAnonymization.anonymize(hostname=_columns[0],
                                               host_salt=_columns[1],
                                               domain_salt=_columns[2],
                                               iterations=iterations)

    _udf = udf(process, StringType())

    return _udf(columns)


def pbkdf2Mapper(columns, iterations, default='', exclude=list()):
    """
    Returns a UDF ready to apply PBKDF2 to a dataframe column, by providing the column's name and parameters.

    :param column: Dataframe column to hash
    :param salt: Salt to use for hashing
    :param iterations: Number of iterations
    :param default: Default hash value if hash can't be applied (e.g. `None` or empty string)
    :param exclude: Values to bypass hashing (hash all by default)
    :return: A PBKDF2 hashed dataframe column

    .. code-block:: python

        dataframe = dataframe.withColumn('host', anonymization.pbkdf2(column=dataframe.host,
                                                                      salt='...',
                                                                      iterations=100))

    """

    def process(x, _salt, _iterations, _default, _exclude):

        if not x:  # not a valid value
            return _default
        else:
            if x in _exclude:  # exclude from hashing
                return x
            else:  # hash
                return hashers.pbkdf2(string=x,
                                      salt=_salt,
                                      iterations=_iterations)

    def udfProcess(columns):
        return process(columns[0], columns[1], iterations, default, exclude)

    _udf = udf(udfProcess, StringType())
    return _udf(columns)

