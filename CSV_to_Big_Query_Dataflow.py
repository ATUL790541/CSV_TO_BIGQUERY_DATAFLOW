import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse
from google.cloud import bigquery

parser = argparse.ArgumentParser()

parser.add_argument('--input',
                      dest='input',
                      required=True,
                      help='Input file to process.')
parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output table to write results to.')

path_args, pipeline_args = parser.parse_known_args()

inputs_pattern = path_args.input
outputs_prefix = path_args.output

options = PipelineOptions(pipeline_args)
p = beam.Pipeline(options=options)



cleaned_data = (
	p
	| beam.io.ReadFromText(inputs_pattern, skip_header_lines=1)
	| beam.Map(lambda row: row.lower())
	| beam.Map(lambda row: row+',1')
)

#BigQuery

client = bigquery.Client()

dataset_id = "gcp-accelerator-380712.housing_data"

dataset = bigquery.Dataset(dataset_id)

dataset.location = "us-central1"
dataset.description = "dataset for housing loan"

dataset_ref = client.create_dataset(dataset, timeout = 30)

def to_json(csv_str):
    fields = csv_str.split(',')

    json_str = {"longitude":fields[0],
                 "latitude": fields[1],
                 "housing_median_age": fields[2],
                 "total_rooms": fields[3],
                 "total_bedrooms": fields[4],
                 "population": fields[5],
                 "households": fields[6],
                 "median_income": fields[7],
                 "median_house_value": fields[8],
                 "ocean_proximity": fields[9]

                 }

    return json_str

table_schema = 'longitude:STRING,latitude:STRING,housing_median_age:STRING,total_rooms:STRING,total_bedrooms:STRING,population:STRING,households:STRING,median_income:STRING,median_house_value:STRING,ocean_proximity:STRING'

(cleaned_data
| 'cleaned_data to json' >> beam.Map(to_json)
| 'write to bigquery' >> beam.io.WriteToBigQuery(
outputs_prefix,
schema=table_schema,
create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND

)

)

from apache_beam.runners.runner import PipelineState
ret = p.run()
