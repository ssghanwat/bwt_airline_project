###
# Author Name : Sandeep Ghanwat
# Creation Date : 15/12/2023
# Last Updated Date : 19/12/2023
# Description : Script is used to generate bronze layer tables based on input datasets
###


from pyspark.sql import SparkSession
import argparse
from datetime import datetime, timedelta
from utils.common_utils import Commonutils


if __name__ == '__main__':
    # Initialize Spark Session

    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # Setting parser creation for add multiple arguments

    parser = argparse.ArgumentParser()
    parser.add_argument('--source_name', default='airport', help='please provide source name/ dataset name')
    parser.add_argument('--schema_base_path',
                        default=r'E:\pyspark\bwt_airline_project\source_schema',
                        help='please provide source name/ dataset name')

    # args contain all parser arguments
    args = parser.parse_args()

    # Initializing argument variables:
    source_name = args.source_name
    schema_base_path = args.schema_base_path

    # Get yesterday's date value in string format (used to find folder name contain date like 20231215 'YYYYMMDD'
    yesterdays_date = datetime.now() - timedelta(days=2)
    yestredays_date = yesterdays_date.strftime("%Y%m%d")

    # Initializing variables
    base_path = r"E:\pyspark\airlineproject_input"
    input_file_path = fr'{base_path}\{source_name}\{yestredays_date}'

    # Creation of object of class Commonutils
    cmnutil = Commonutils()

    # calling generate_bronze_schema method to generate schema format
    input_schema = cmnutil.generate_bronze_schema(schema_base_path, source_name)
    # print(input_schema)

    # Create dataframe on input file and schema generated in above calling
    df = spark.read.csv(input_file_path, input_schema)
    # df.show(1)
    # print(df.printSchema())

    # Generating list of columns on which special char check we want to apply
    # compulsory for both the special char methods defined in Commonutils class
    list_of_columns = cmnutil.columns_to_check_special_char(schema_base_path, source_name)
    # print(list_of_columns)

    #  #########  individual special characters columns code      ###########

    # If we want to show individual special characters columns then uncomment below code
    # Note : Respective method w.r.t. to this should be uncommented.
    # for i in list_of_columns:
    #     df = cmnutil.special_char_check(df,i)
    # df.show()

    # ########### Generating unified column of special char for finding out bad record #################
    # If we want to show unified special char column just for finding out bad record uncomment below code
    # Note : Respective method w.r.t. to this should be uncommented.
    df = cmnutil.special_char_check(df, list_of_columns)
    df.show()

    # filtering good data from data frame
    good_data = df.filter(df.special_char == '')
    final_good_data = good_data.drop(df.special_char)  # used to drop special char columns

    # setting base path for write file on required destination
    base_path_for_output = r'E:\pyspark\airlineproject_input'
    layer_name = 'bronze'

    # writing good data on parquet file at respective location
    final_good_data.write.csv(fr'{base_path_for_output}\{layer_name}\{source_name}\{yestredays_date}\good_data',
                                  mode='overwrite')

    # filtering good data from data frame
    bad_data = df.filter(df.special_char != '')
    final_bad_data = good_data.drop(df.special_char)  # used to drop special char columns

    # writing good data on parquet file at respective location
    final_bad_data.write.csv(fr'{base_path}\{source_name}\{yestredays_date}\bad_data', mode='overwrite')

