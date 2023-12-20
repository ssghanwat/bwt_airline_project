from pyspark.sql.functions import *
from pyspark.sql.types import *


class Commonutils:
    @staticmethod
    def generate_bronze_schema(schema_base_path, source_name):
        struct_field_list = []
        try:
            with open(fr'{schema_base_path}\{source_name}_schema.csv', 'r') as r:
                # print(r.tell())
                for j in r:
                    column_name = j.split(',')[0]
                    # data_types = j.split(',')[1]
                    struct_field_list.append(StructField(column_name, StringType()))
                input_schema = StructType(struct_field_list)
        except Exception as e:
            raise Exception(f'Error while executing generate_bronze_schema method as {e}')
        # r.seek(0)
        # print(r.tell()
        # print(r.closed)
        return input_schema

    @staticmethod
    def columns_to_check_special_char(schema_base_path, source_name):
        columns = []
        try:
            r = open(fr'{schema_base_path}\{source_name}_schema.csv', 'r')
            for j in r:
                if 'special_char' in j:
                    columns.append(j.split(',')[0])
        except Exception as e:
            raise Exception(f'Error while executing generate_bronze_schema method as {e}')
        return columns

        ##########  individual special characters columns method     ###########

        # If we want to show individual special characters columns then uncomment below method
        # Note respective code w.r.t to this method in bronze file should be uncommented
    @staticmethod
    def special_char_check(df, col_name):
        special_char_check = r"([^A-Za-z|0-9|/|\s*])"
        try:
            df = df.withColumn(f"{col_name}_special_char", lit(""))
            df = df.withColumn(f"{col_name}_special_char", when(col(f"{col_name}_special_char") == '',
                    regexp_extract(f"{col_name}", special_char_check,1)).otherwise(
                            col(f"{col_name}_special_char")))

        except Exception as e:
            raise (f'Error while executing generate_bronze_schema method as {e}')
        return df

        ############ Generating unified column of special char for finding out bad record #################

    @staticmethod
    def special_char_check(df,list_of_columns):
        special_char_check = r"([^A-Za-z|0-9|/|\s*])"
        try:
            df = df.withColumn(f"special_char", lit(""))
            for i in list_of_columns:
                df = df.withColumn("special_char", when(col("special_char") == '',
                                    regexp_extract(f"{i}", special_char_check, 1)).otherwise(
                                        col("special_char")))
        except Exception as e:
            raise Exception(f'Error while executing generate_bronze_schema method as {e}')
        return df
