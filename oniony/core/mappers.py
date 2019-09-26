import uuid
import os

import binascii
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, ByteType


class UUIDMapper:
    """
    Class to create a mapping between original UUIDs and anonymized UUIDs.
    Given a column containing UUIDs, new UUIDs will be generated if not present in the original mapping.
    .. code-block:: python
        >> anon.anonymized.show()
        +--------------------+--------------------+------------+-------------+
        |       original_uuid|        created_date|created_year|created_month|
        +--------------------+--------------------+------------+-------------+
        |c7916a3d-a5ec-409...|2016-10-04 00:50:...|        2016|           10|
        |3d479685-f3b7-422...|2016-10-04 00:59:...|        2016|           10|
        |9368a48d-cc35-44f...|2016-10-04 01:12:...|        2016|           10|
        +--------------------+--------------------+------------+-------------+
        >> mapper = UUIDMapper(sc=spark, column_name='original_uuid')
        >> mapper.add(anon.anonymized.select('original_uuid'))
        >> mapper.mappings.show()
        +--------------------+--------------------+
        |       original_uuid|          anonymized|
        +--------------------+--------------------+
        |00566729-d814-484...|b99d695a-5594-444...|
        |0065e34d-17d1-45b...|935102b7-9402-426...|
        |018f9fb3-763f-48f...|77e31ced-fcde-458...|
        +--------------------+--------------------+
    """

    ANONYMIZED_COLUMN = 'anonymized'

    def __init__(self, sc, column_name):
        """
        :param sc: A Spark context
        :param column_name: Identifier for the UUID column.
        """
        self._sc = sc
        self._column_name = column_name

        _schema = StructType([
            StructField(column_name, StringType(), False),
            StructField(UUIDMapper.ANONYMIZED_COLUMN, StringType(), False)
        ])

        self.mappings = sc.createDataFrame([], _schema)

    def add(self, column):
        """
        Adds a dataframe column containing raw UUIDs to the mapper.
        Existing UUIDs will not be added, new UUIDs will be assigned a new anonymised UUID.
        :param column: Spark Dataframe column containing UUIDs
        :return: The current mappings after the operation
        """
        _mapped = self.mappings.select(self._column_name)
        _unseen = column.subtract(_mapped)

        _udf = udf(lambda x: str(uuid.uuid4()), returnType=StringType())

        _unseen = _unseen.withColumn(UUIDMapper.ANONYMIZED_COLUMN, _udf(_unseen[self._column_name]))

        self.mappings = self.mappings.union(_unseen)

        return self.mappings


class SaltMapper:
    """
    Class to create a mapping between a reference column and a salt value.
    Given a column containing reference values (UUIDs, account numbers, etc), random salt values will be generated
    if not present in the original mapping.
    .. code-block:: python
        >> anon.anonymized.show()
        +--------------------+--------------------+------------+-------------+
        |       original_uuid|        created_date|created_year|created_month|
        +--------------------+--------------------+------------+-------------+
        |c7916a3d-a5ec-409...|2016-10-04 00:50:...|        2016|           10|
        |3d479685-f3b7-422...|2016-10-04 00:59:...|        2016|           10|
        |9368a48d-cc35-44f...|2016-10-04 01:12:...|        2016|           10|
        +--------------------+--------------------+------------+-------------+
        >> mapper = SaltMapper(sc=spark, original_column_name='original_uuid', value_column_name='host_salt')
        >> mapper.add(anon.anonymized.select('original_uuid'))
        >> mapper.mappings.show()
        +--------------------+--------------------+
        |       original_uuid|           host_salt|
        +--------------------+--------------------+
        |00566729-d814-484...|8b57ac30d98ca074b...|
        |0065e34d-17d1-45b...|7c61088d9d2fa509e...|
        |018f9fb3-763f-48f...|d28d52fb03ccaeee1...|
        +--------------------+--------------------+
    """

    def __init__(self, sc, reference, value):
        """
        :param sc: A Spark context
        :param reference: Identified for the reference column.
        :param value: Value associated with the reference
        """
        self._sc = sc
        self.reference = reference
        self.value = value

        _schema = StructType([
            StructField(reference, StringType(), False),
            StructField(value, StringType(), False)
        ])

        self.mappings = sc.createDataFrame([], _schema)

    def add(self, column):
        """
        Adds a dataframe column containing raw reference values to the mapper.
        Existing reference values will not be added, new values will be assigned a randomized salt.
        :param column: Spark Dataframe column containing reference values
        :return: The current mappings after the operation
        """
        _mapped = self.mappings.select(self.reference)
        _unseen = column.subtract(_mapped)

        _udf = udf(lambda x: binascii.hexlify(os.urandom(16)), returnType=StringType())

        _unseen = _unseen.withColumn(self.value, _udf(_unseen[self.reference]))

        self.mappings = self.mappings.union(_unseen)

        return self.mappings