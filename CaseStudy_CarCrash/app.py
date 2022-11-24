from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number
from loadfiles import from_files
import transform
import utils

class CarCrashAnalysis:
    def __init__(self, yaml_config_file):
        input_file_location = utils.read_yaml(yaml_config_file).get("INPUT_FILENAME")
        self.df_charges = from_files(spark,input_file_location,"Charges")
        self.df_damages =  from_files(spark,input_file_location,"Damages")
        self.df_endorse =  from_files(spark,input_file_location,"Endorse")
        self.df_primary_person =  from_files(spark,input_file_location,"Primary_Person")
        self.df_units = from_files(spark,input_file_location,"Units")
        self.df_restrict = from_files(spark,input_file_location,"Restrict")

if __name__ == '__main__':
    # Initialize sparks session
    spark = SparkSession \
        .builder \
        .appName("CarCrashAnalysis") \
        .getOrCreate()

    yaml_config_file = "config.yaml"
    spark.sparkContext.setLogLevel("ERROR")

    cca = CarCrashAnalysis(yaml_config_file)
    output_file_paths = utils.read_yaml(yaml_config_file).get("OUTPUT_PATH")
    file_format = utils.read_yaml(yaml_config_file).get("FILE_FORMAT")
    
    #Output the Analysis Result
    print("Analysis 1. Output: ", transform.crashes_with_males_killed(cca.df_primary_person,output_file_paths.get(1), file_format.get("Output")))

    print("Analysis 2. Output: ", transform.crashes_count_two_wheeler_accidents(cca.df_units,output_file_paths.get(2), file_format.get("Output")))

    print("Analysis 3. Output: ", transform.get_statesname_with_highest_female_accidents(cca.df_primary_person,output_file_paths.get(3),
                                                                     file_format.get("Output")))

    print("Analysis 4. Output: ", transform.get_top_5to15_vehicle_with_highestinjuries(cca.df_units,output_file_paths.get(4),
                                                                       file_format.get("Output")))

    print("Analysis 5. Output: ")
    transform.get_ethnic_ug_crash_bodystyle_crashed(cca.df_units,cca.df_primary_person,output_file_paths.get(5), file_format.get("Output"))

    print("Analysis 6. Output: ", transform.get_top5_zips_with_alcohols_as_crashresult(cca.df_units,cca.df_primary_person,output_file_paths.get(6),
                                                                                file_format.get("Output")))

    print("Analysis 7. Output: ", transform.get_uniquecrash_ids_with_nodamage(cca.df_units,cca.df_damages,output_file_paths.get(7), file_format.get("Output")))

    print("8. Output: ", transform.get_top5_vehicle_make(cca.df_units,cca.df_primary_person,cca.df_charges,output_file_paths.get(8), file_format.get("Output")))

    spark.stop()
