import configparser

#config = ConfigParser.RawConfigParser()
#config = configparser.RawConfigParser()
config = configparser.ConfigParser()
config.read('/Application/rep_preload/conf/rep_preload.conf')

def initConfig():
    config = ConfigParser.RawConfigParser()
    config.read('/Application/rep_preload/conf/rep_preload.conf')
    return config