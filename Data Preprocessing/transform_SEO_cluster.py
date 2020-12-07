import os, datetime
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


# DoFn to perform on each element in the input PCollection.
class Separate(beam.DoFn):
    
    # Separate one column of description to two columns
    # The half before '(' goes to the indicator name column, the other half goes to the detail column
    def process(self, element):
        record = element
        SEO = record.get('SEO')
        name = record.get('Socio_economic_objective')
        
        if ': ' in name:
            sep = name.split(': ')
            SEO_name = sep[0]
            source = sep[1][0].upper() + sep[1][1:]
        elif ' - ' in name:
            sep = name.split(' - ')
            SEO_name = sep[0]
            source = sep[1][0].upper() + sep[1][1:]
        else:
            SEO_name = name
            source = 'N/A'
            
            
        row = {'SEO': SEO, 'Socio_economic_objective': SEO_name, 'Funding_source': source}
            
        return [row]


PROJECT_ID = os.environ['PROJECT_ID']
BUCKET = os.environ['BUCKET']
DIR_PATH = BUCKET + '/output/' + datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

# run pipeline on Dataflow 
options = {
    'runner': 'DataflowRunner',
    'job_name': 'transform-takes-table',
    'project': PROJECT_ID,
    'temp_location': BUCKET + '/temp',
    'staging_location': BUCKET + '/staging',
    'machine_type': 'n1-standard-1', # machine types listed here: https://cloud.google.com/compute/docs/machine-types
    'num_workers': 1
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DataflowRunner', options=opts) as p:
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset_2.SEO'))

    # write PCollection to log file
    query_results | 'Write to file' >> WriteToText('Input.txt')

    # apply a ParDo to the PCollection
    indicator_pcoll = query_results | 'Direct' >> beam.ParDo(Separate())


    # write PCollection to a file
    indicator_pcoll | 'Write to File' >> WriteToText('Output.txt')
    
    # make BQ records    
    qualified_table_name =  PROJECT_ID + ':dataset_2.SEO_cluster'
    table_schema = 'SEO:STRING,Socio_economic_objective:STRING,Funding_source:STRING'
    
    indicator_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))



