from pyspark.sql.functions import udf, struct, col
from pyspark.sql.types import MapType, StringType

from oniony import Anonymizer
from oniony.core.mappers import SaltMapper
from oniony.udfs.anonymization import pbkdf2Mapper
from oniony.udfs import anonymization, restructure


class Hostname(Anonymizer):
    def __init__(self, sc, dataframe, host_salt, domain_salt, iterations):
        """
        :param sc: A Spark context
        :param dataframe:  The Spark dataframe to process
        :param host_salt: Hashing salt for the host portion of the hostname
        :param domain_salt: Hashing salt for the domain portion of the hostname
        """
        self.host_salt = host_salt
        self.domain_salt = domain_salt
        self.iterations = iterations

        Anonymizer.__init__(self, context=sc, dataframe=dataframe)

    def restructure(self, dataframe):
        parse_udf = udf(lambda x: Hostname.parse(x), MapType(StringType(), StringType()))

        dataframe = dataframe.withColumn('content', parse_udf(dataframe.content))
        dataframe = restructure.expandName(dataframe, dataframe.content, ['domain', 'host'])
        dataframe = dataframe.drop('content')

        return dataframe

    def anonymize(self, dataframe):

        if isinstance(self.host_salt, SaltMapper):

            # add reference field to the mapper
            self.host_salt.add(dataframe.select(self.host_salt.reference))

            # join values using reference
            joined = dataframe.alias('df').join(self.host_salt.mappings.alias('mappings'),
                                                col('df.' + self.host_salt.reference) == col(
                                                    'mappings.' + self.host_salt.reference)) \
                .drop(col('mappings.' + self.host_salt.reference))

            dataframe = joined.withColumn('host', pbkdf2Mapper(struct(joined['host'], joined[self.host_salt.value]),
                                                               self.iterations)) \
                .drop(col('mappings.' + self.host_salt.value))

        elif isinstance(self.host_salt, str):

            dataframe = dataframe.withColumn('host', anonymization.pbkdf2(column=dataframe.host,
                                                                          salt=self.host_salt,
                                                                          iterations=self.iterations))

        else:
            raise ValueError('salt must be an instance of SaltMapper or a string')

        if isinstance(self.domain_salt, SaltMapper):

            # add reference field to the mapper
            self.domain_salt.add(dataframe.select(self.domain_salt.reference))

            # join values using reference
            joined = dataframe.alias('df').join(self.domain_salt.mappings.alias('mappings'),
                                                col('df.' + self.domain_salt.reference) == col(
                                                    'mappings.' + self.domain_salt.reference)) \
                .drop(col('mappings.' + self.domain_salt.reference))

            dataframe = joined.withColumn('domain',
                                          pbkdf2Mapper(struct(joined['domain'], joined[self.domain_salt.value]),
                                                       self.iterations)) \
                .drop(col('mappings.' + self.domain_salt.value))

        elif isinstance(self.domain_salt, str):

            dataframe = dataframe.withColumn('domain', anonymization.pbkdf2(column=dataframe.domain,
                                                                            salt=self.domain_salt,
                                                                            iterations=self.iterations))

        else:
            raise ValueError('salt must be an intance of SaltMapper or a string')

        return dataframe

    @staticmethod
    def parse(content):
        return {'domain': ".".join(content.split(".")[1:]) if content else None, 'host': content}
