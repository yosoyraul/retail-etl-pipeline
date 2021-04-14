from configparser import ConfigParser

def read_db_config(filename='config.ini',section='mysql'):
    """ Read database configuration file and return a dictionary object
    :param filename: name of the configuration file
    :param section: section of database configuration
    :return: a dictionary of database parameters
    """

    parser = ConfigParser()
    parser.read(filename)

    configs = {}
    if parser.items(section):
        items = parser.items(section)
        for item in items:
            configs[item[0]] = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section,filename))

    return configs