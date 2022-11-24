import utils
def from_files(spark,input_file_paths,filename):
    df = spark.read. \
         option("inferSchema", "true"). \
         csv(input_file_paths.get(filename), header=True)
    return df