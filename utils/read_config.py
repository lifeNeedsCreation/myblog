import os
from configparser import ConfigParser, ExtendedInterpolation

DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(DIR, '../config/configs.ini')
CONFIG = ConfigParser(interpolation=ExtendedInterpolation())
CONFIG.read(CONFIG_PATH)