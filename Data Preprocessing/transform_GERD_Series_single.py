import os
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


# DoFn to perform on each element in the input PCollection.
class Separate(beam.DoFn):
    
    # Separate one column of description to two columns
    # The half before '(' goes to the indicator name column, the other half goes to the detail column
    def process(self, element):
        record = element
        Indicator_Code = record.get('Indicator_Code')
        name = record.get('Indicator_Name')
        rest = ''
        for i in range (len(name)):
            if name[i] == '(' or name[i] == '%':
                Indicator_Name = name[:i].strip()
                rest = name[i:]
                break
            
        if rest == '':
            Detail = None
            Indicator_Name = name
        elif rest == '%':
            Detail = rest
        else:
            if rest[-1] != '%':
                Detail = rest.replace('(','').replace(')',',')[:-1]
                Detail = Detail[0].upper() + Detail[1:]
            else:
                Detail = rest.replace('(','').replace(')',',')
                Detail = Detail[0].upper() + Detail[1:]
            
        row = {'Indicator_Code': Indicator_Code, 'Indicator_Name': Indicator_Name, 'Detail': Detail}
            
        return [row]


PROJECT_ID = os.environ['PROJECT_ID']

# Project ID is needed for BigQuery data source, even for local execution.
options = {
    'project': PROJECT_ID
}
opts = beam.pipeline.PipelineOptions(flags=[], **options)

# Create a Pipeline using a local runner for execution.
with beam.Pipeline('DirectRunner', options=opts) as p:
    query_results = p | 'Read from BigQuery' >> beam.io.Read(beam.io.BigQuerySource(query='SELECT * FROM dataset_2.GERD_Series'))

    # write PCollection to log file
    query_results | 'Write to file' >> WriteToText('Input.txt')

    # apply a ParDo to the PCollection
    indicator_pcoll = query_results | 'Direct' >> beam.ParDo(Separate())


    # write PCollection to a file
    indicator_pcoll | 'Write to File' >> WriteToText('Output.txt')
    
    # make BQ records    
    qualified_table_name =  PROJECT_ID + ':dataset_2.GERD_Indicators'
    table_schema = 'Indicator_Code:STRING,Indicator_Name:STRING,Detail:STRING'
    
    indicator_pcoll | 'Write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(qualified_table_name, 
                                                    schema=table_schema,  
                                                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))



