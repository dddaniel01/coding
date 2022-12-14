"""A common words task."""

#   name: CommonWords
#   dev: Daniel Castillo Torres
#   date: September 7, 2022

#   Description:
#   Use Apache Beam Python SDK (https://beam.apache.org/)
#   You should be able to run it locally, don’t need to serve it in the cloud. Jupiter notebook is a
#   good choice.
#   We want to process a file and find
#       - Most common word
#       - Least common word
#   Please notice that counting should be case insensitive.
#   Input and Output paths should be parameters but some default values should be provided.

#   Example
#       input.txt:
#       aa Ab ab Ab C CD
#       most_common.txt
#       ab
#       least_common.txt
#       aa
#       —
#       (c and cd are also valid outputs)

import argparse
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default='letter.txt',
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      default='outputs',
      help='Output directory to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)

  # The pipeline will be run.
  with beam.Pipeline(options=pipeline_options) as p:

    lines = p | 'Read' >> ReadFromText(known_args.input)

    counts = (
        lines
        | 'Split' >> (
            beam.FlatMap(
                lambda x: re.findall(r'[A-Za-z\']+', x.lower(), re.IGNORECASE)).with_output_types(str))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'CombineAndSum' >> beam.CombinePerKey(sum)
    )
    
    values = (
        counts
        | 'GetValues' >> beam.Values()
    )

    max_value = (
        values
        | 'GetMaxValue' >>
      beam.CombineGlobally(lambda elements: max(elements or [None]))
    )

    min_value = (
        values
        | 'GetMinValue' >>
      beam.CombineGlobally(lambda elements: min(elements or [None]))
    )

    most_common= (
        counts
        | 'GetMostCommon' >> beam.Filter(lambda word, mv: word[1] == mv, mv=beam.pvalue.AsSingleton(max_value)) 
        | 'GetLatestElementFromMostCommon' >> beam.combiners.Latest.Globally()
    )

    least_common= (
        counts
        | 'GetLestCommon' >> beam.Filter(lambda word, mv: word[1] == mv, mv=beam.pvalue.AsSingleton(min_value))
        | 'GetLatestElementFromLeastCommon' >> beam.combiners.Latest.Globally()
    )

    # Format the counts 
    def format_result(word, _):
      return f'{word}'

    output_most = most_common | 'FormatMost' >> beam.MapTuple(format_result)
    output_least = least_common | 'FormatLeast' >> beam.MapTuple(format_result)

    # Write the output 
    output_most | 'WriteMost' >> WriteToText(known_args.output + "/most_common.txt")
    output_least | 'WriteLeast' >> WriteToText(known_args.output + "/least_common.txt")

if __name__ == '__main__':
  run()
