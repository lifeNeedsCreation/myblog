import os
from configparser import ConfigParser, ExtendedInterpolation

BASEDIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASEDIR, '../config/configs.ini')
CONFIG = ConfigParser(interpolation=ExtendedInterpolation())
CONFIG.read(CONFIG_PATH)