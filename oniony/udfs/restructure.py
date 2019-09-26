def expandIndex(dataframe, column, mapping):
    """
    Expand a Spark dataframe column of `ArrayType` into separate columns specified by the `mapping`

    .. code-block:: python

        +--------------------+-----------------------+
        |     attachment_uuid|                content|
        +--------------------+-----------------------+
        |c7916a3d-a5ec-409...|['red', 'blue', 'green'|
        +--------------------+-----------------------+

        >> dataframe = restructure.expandIndex(dataframe,
           dataframe.content, {'colour1': 0, 'colour2': 1, 'colour3': 2}).show()

        +--------------------+----------+----------+----------+
        |     attachment_uuid|   colour1|   colour2|   colour3|
        +--------------------+----------+----------+----------+
        |c7916a3d-a5ec-409...|     'red'|    'blue'|   'green'|
        +--------------------+----------+----------+----------+


    :param dataframe: The Spark dataframe to process
    :param column: The column to process
    :param mapping: A dictionary contaning `{destination_column : index}`
    :return:
    """
    for name, index in iter(mapping.items()):
        dataframe = dataframe.withColumn(name, column.getItem(index))
    return dataframe


def expandName(dataframe, column, names):
    """
    Expand a Spark dataframe column of `MapType` into separate columns specified by the `mapping`

    .. code-block:: python

       +--------------------+-----------------------------------+
       |     attachment_uuid|                            content|
       +--------------------+-----------------------------------+
       |c7916a3d-a5ec-409...|{c1: 'red', c2: 'blue', c3: 'green'|
       +--------------------+-----------------------------------+

       >> dataframe = restructure.expandIndex(dataframe,
          dataframe.content, {'colour1': 'c1', 'colour2': 'c2', 'colour3': 'c3'}).show()

       +--------------------+----------+----------+----------+
       |     attachment_uuid|   colour1|   colour2|   colour3|
       +--------------------+----------+----------+----------+
       |c7916a3d-a5ec-409...|     'red'|    'blue'|   'green'|
       +--------------------+----------+----------+----------+


    :param dataframe: The Spark dataframe to process
    :param column: The column to process
    :param mapping: A dictionary contaning `{destination_column : key}`
    :return:
    """
    for name in names:
        dataframe = dataframe.withColumn(name, column.getItem(name))
    return dataframe
