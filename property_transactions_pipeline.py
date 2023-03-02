# data items 
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

class CleanFirstColumn(beam.DoFn):
    """
    A DoFn that removes the curly braces from the first column of a CSV row.
    """
    def process(self, element):
        element[0] = element[0].replace('{', '').replace('}', '')
        yield element

class MakeDictionary(beam.DoFn):
    """
    A DoFn that converts a CSV row into a dictionary given the list of column headers
    """
    def process(self, element):
        # Combine each column header with its corresponding value
        zipped = zip(cols, element)
        # Convert the result into a dictionary
        row_dict = dict(zipped)
        yield row_dict

class CreateKey(beam.DoFn):
    """
    A DoFn that creates a key for grouping by concatenating the PAON, SAON, Street, and Postcode columns of a row.
    """
    def process(self, element):
        key = element['PAON'] + ',' + element['SAON'] + ',' + element['Street'] + ',' + element['Postcode']
        yield (key, element)

# Define the pipeline
with beam.Pipeline() as pipeline:
    (
        pipeline
        | 'Read CSV' >> beam.io.ReadFromText('pp-monthly-update-new-version.csv')
        | "Create list of lists for each transaction" >> beam.Map(lambda line: next(csv.reader([line])))
        | "Clean First Column" >> beam.ParDo(CleanFirstColumn())
        | "Make each transaction an object" >> beam.ParDo(MakeDictionary())
        | 'Create address key for grouping ' >> beam.ParDo(CreateKey())
        | 'Group By address key' >> beam.GroupByKey()
        | 'Convert to JSON' >> beam.Map(json.dumps)
        | 'Write JSON' >> beam.io.WriteToText('output',file_name_suffix=".json", shard_name_template="",num_shards=1)
    )
