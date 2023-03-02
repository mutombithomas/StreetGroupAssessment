import apache_beam as beam
import csv
import json
import datetime
import logging
import uuid


class DataProcessor:
    """
    A class to process data using Apache Beam.
    Attributes:
        cols (list): A list of column names.
    """

    def __init__(self, cols):
        """
        Initializes the DataProcessor class with the given list of column names.
        Args:
            cols (list): A list of column names.
        """
        self.cols = cols

    class CleanFirstColumn(beam.DoFn):
        """
        A class to clean the first column of a row.
        """
        def process(self, element):
            """
            Removes the curly braces from the first element in the row.
            Args:
                element (list): A list representing a row of data.
            Returns:
                A list with the first element modified.
            """
            element[0] = element[0].replace('{', '').replace('}', '')
            yield element

    class MakeDictionary(beam.DoFn):
        """
        A class to convert a row of data into a dictionary.
        """
        def __init__(self, cols):
            """
            Initializes the MakeDictionary class with the given list of column names.
            Args:
                cols (list): A list of column names.
            """
            super().__init__()
            self.cols = cols

        def process(self, element):
            """
            Combines each column header with its corresponding value, and converts the result into a dictionary.
            Args:
                element (list): A list representing a row of data.
            Returns:
                A dictionary representing the row of data.
            """
            zipped = zip(self.cols, element)
            row_dict = dict(zipped)
            yield row_dict

    class CreateKey(beam.DoFn):
        """
        A class to create a unique key for each row of data based on the address.
        """
        def process(self, element):
            """
            Combines the PAON, SAON, Street, and Postcode columns into a unique key.
            Args:
                element (dict): A dictionary representing a row of data.
            Returns:
                A tuple representing the key and the row of data.
            """
            key = element['PAON'] + ',' + element['SAON'] + ',' + element['Street'] + ',' + element['Postcode']
            yield (key, element)

    class CheckRowLength(beam.DoFn):
        """
        A class to check the length of each row of data.
        """
        def process(self, element):
            """
            Checks if the length of the row is equal to the expected number of columns.
            Args:
                element (list): A list representing a row of data.
            Returns:
                If the length is correct, yields the row of data. Otherwise, logs an error message.
            """
            expected_length = 16  # The expected number of columns in each row
            if len(element) != expected_length:
                logging.error(f"Row {element} does not have {expected_length} columns.")
                return
            yield element

    class CheckDataTypes(beam.DoFn):
        """
        DoFn for checking the data types of each element in the PCollection.
        """
        def process(self, element):
            """
            Check if the data types of each element in the PCollection are as expected.
            Args:
                element: The element in the PCollection to be checked.
            Yields:
                The element if its data types are as expected.
            Raises:
                ValueError: If the data types of an element in the PCollection are not as expected.
            """
            try:
                uuid.UUID(element[0])  # Check if the first column is a UUID
                int(element[1])  # Check if the second column is an integer
                datetime.datetime.strptime(element[2], '%Y-%m-%d %H:%M')  # Check if the third column is a datetime
                # Check if the other columns have the expected data types
                # ...
            except ValueError as e:
                logging.error(f"Row {element} has invalid data types: {e}")
                return
            yield element


    def process_data(self, input_file, output_file):
        """
        Main function for processing data.
        Args:
            input_file: The path to the input file.
            output_file: The path to the output file.
        """
        # Define the pipeline
        with beam.Pipeline() as pipeline:
            (
                pipeline
                | 'Read CSV' >> beam.io.ReadFromText(input_file)
                | 'Create list of lists for each transaction' >> beam.Map(lambda line: next(csv.reader([line])))
                | 'Data Quality Check: Row length' >> beam.ParDo(self.CheckRowLength())
                | 'Data Quality Check: Data types' >> beam.ParDo(self.CheckDataTypes())
                | 'Clean First Column' >> beam.ParDo(self.CleanFirstColumn())
                | 'Make each transaction an object' >> beam.ParDo(self.MakeDictionary(self.cols))
                | 'Create address key for grouping' >> beam.ParDo(self.CreateKey())
                | 'Group by address key' >> beam.GroupByKey()
                | 'Convert to JSON' >> beam.Map(json.dumps)
                | 'Write JSON' >> beam.io.WriteToText(output_file, file_name_suffix='.json', num_shards=1,shard_name_template="")
            )

            
            
########## Run pipeline #########
cols = [
    'ID',
    'Price',
    'TransferDate',
    'Postcode',
    'PropertyType',
    'IsNew',
    'Duration',
    'PAON',
    'SAON',
    'Street',
    'Locality',
    'TownOrCity',
    'District',
    'County',
    'PPDCategoryType',
    'RecordStatus'
]

processor = DataProcessor(cols)

input_file = 'pp-monthly-update-new-version.csv'
output_file = 'output'

processor.process_data(input_file, output_file)
