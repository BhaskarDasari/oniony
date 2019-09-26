class Anonymizer:
    def __init__(self, context, dataframe):

        restructed = self.restructure(dataframe)

        self._anonymized = self.anonymize(restructed)

    def restructure(self, dataframe):
        """
        Applies the restructuring Spark transformations
        :param dataframe: The original Spark dataframe
        :return: A restructured Spark dataframe
        """
        raise NotImplementedError()

    def anonymize(self, dataframe):
        """
        Applies the anonymization Spark transformations
        :param dataframe: A restructured Spark dataframe
        :return: An anonymized Spark dataframe
        """
        raise NotImplementedError()

    @property
    def anonymized(self):
        return self._anonymized