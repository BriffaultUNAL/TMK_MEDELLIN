#!/usr/bin/python

from src.utils import *
import sys
import os


act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, 'src')
sys.path.append(proyect_dir)


if __name__ == "__main__":

    webscraping(**source2)
    load()
    load_param()
