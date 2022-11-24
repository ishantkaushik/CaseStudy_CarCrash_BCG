import yaml

def read_yaml(file_path):
    """
    Read Config file in YAML format
    :param file_path: file path to config.yaml
    :return: dictionary with config details
    """
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


def write_output(df, file_path, write_format):
    df.write. \
    format(write_format). \
    mode('overwrite'). \
    option("header", "true"). \
    save(file_path)
