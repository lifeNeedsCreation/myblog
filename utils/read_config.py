import os, sys
from configparser import ConfigParser, ExtendedInterpolation

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

CONFIG_PATH = os.path.join(BASE_DIR, 'config/configs.ini')
CONFIG = ConfigParser(interpolation=ExtendedInterpolation())
ONFIG.read(CONFIG_PATH)