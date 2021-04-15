# -*- coding:utf-8 -*-
import os
from functools import partial

BLITZ_COOKBOOK_DEFAULT_DIR = os.path.dirname(__file__)

cookbook_path = partial(os.path.join, BLITZ_COOKBOOK_DEFAULT_DIR)
