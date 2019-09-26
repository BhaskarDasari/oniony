import ipaddress
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import ArrayType, StringType

from oniony.anonymizer import IPAnonymization, HostnameAnonymization
from oniony import Anonymizer
from oniony.udfs import restructure


class Hosts(Anonymizer):
    """
    This class anonymizes dataframes containing `hosts` information.

    Typically, the `content` column will contain the entirety of the `hosts` file.
    The resulting dataframe will contain one row per `hosts` entry.

    :param sc: The spark context to be used
    :param host_salt: Salt used for hashing the host part of the hostname
    :param domain_salt: Salt used for hashing the domain part of the hostname
    :param aes_key: Key for IP anonymization
    :param padding_key: Key for IP anonymization
    :param iterations: Number of passes for the hashing algorithm
    :param excluded_ips: `hosts` entries containing an IP address in this list will not be anonymized

    .. code-block:: python

        anon = Hosts(sc=spark,
                     host_salt='...'
                     domain_salt='...',
                     aes_key='...',
                     padding_key='...',
                     iterations=100,
                     excluded_ips=['127.0.0.1', '::1']

    """

    def __init__(self,
                 sc,
                 dataframe,
                 host_salt,
                 domain_salt,
                 aes_key,
                 padding_key,
                 iterations,
                 excluded_ips=None):

        if excluded_ips is None:
            excluded_ips = []
        self.host_salt = host_salt
        self.domain_salt = domain_salt
        self.aes_key = aes_key
        self.padding_key = padding_key
        self.iterations = iterations
        self.excluded_ips = excluded_ips

        Anonymizer.__init__(self, context=sc, dataframe=dataframe)

    def restructure(self, dataframe):

        _udf = udf(lambda x: Hosts.parse_ip(x, self.aes_key, self.padding_key, self.excluded_ips),
                   returnType=StringType())

        dataframe = dataframe.withColumn('content', _udf(dataframe.content))

        return dataframe

    def anonymize(self, dataframe):

        return dataframe

    @staticmethod
    def parse_ip(content, aes_key, padding_key, excluded_ips):

        try:  # try for valid ip address
            ipaddress.ip_address(content)
            if content not in excluded_ips:
                return IPAnonymization.anonymize(content, aes_key=aes_key, padding_key=padding_key)
        except ValueError:
            return None
