import apache_beam as beam
import argparse
from apache_beam.options.pipeline_options import PipelineOptions
from sys import argv

PROJECT_ID = 'aniket-g'
SCHEMA = 'sr:INTEGER,abv:FLOAT,id:INTEGER,name:STRING,style:STRING,ounces:FLOAT'

def convert_types(data):
    """Converts string values to their appropriate type."""

    data['id'] = int(data['id']) if 'id' in data else None
    data['name'] = str(data['name']) if 'name' in data else None
    data['host_id'] = int(data['host_id']) if 'host_id' in data else None
    data['host_name'] = str(data['host_name']) if 'host_name' in data else None
    data['neighbourhood_group'] = str(data['neighbourhood_group']) if 'neighbourhood_group' in data else None

    data['neighbourhood'] = str(data['neighbourhood']) if 'neighbourhood' in data else None
    data['latitude'] = float(data['latitude']) if 'latitude' in data else None
    data['longitude'] = float(data['longitude']) if 'longitude' in data else None
    data['room_type'] = str(data['room_type']) if 'room_type' in data else None
    data['price'] = int(data['price']) if 'price' in data else None

    data['minimum_nights'] = int (data['minimum_nights']) if 'minimum_nights' in data else None
    data['number_of_reviews'] = int (data['number_of_reviews']) if 'number_of_reviews' in data else None
    data['last_review'] = str (data['last_review']) if 'last_review' in data else None
    data['reviews_per_month'] = float (data['reviews_per_month']) if 'reviews_per_month' in data else None
    data['calculated_host_listings_count'] = int (data['calculated_host_listings_count']) if 'calculated_host_listings_count' in data else None

    data['availability_365'] = int (data['availability_365']) if 'availability_365' in data else None
    return data

def groupby_neighbourhood(data):
    """groupby neighbourhoods in NewYork"""
    del data['ibu']
    del data['brewery_id']
    return data

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    known_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions())

    (p | 'ReadData' >> beam.io.ReadFromText('gs://ag-pipeline/batch/beers.csv', skip_header_lines =1)
       | 'SplitData' >> beam.Map(lambda x: x.split(','))
       | 'FormatToDict' >> beam.Map(lambda x: {"sr": x[0], "abv": x[1], "ibu": x[2], "id": x[3], "name": x[4], "style": x[5], "brewery_id": x[6], "ounces": x[7]})
       | 'ChangeDataType' >> beam.Map(convert_types)
       | 'groubyNeighbourhood' >> beam.GroupByKey()
       | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:beer.beer_data'.format(PROJECT_ID),
           schema=SCHEMA,
           write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    result = p.run()

