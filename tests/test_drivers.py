"""
Unittests for gpsd_format.drivers
"""


import unittest

import newlinejson

from gpsd_format import drivers
from .sample_files import *


class TestNewlineJSON(unittest.TestCase):

    def test_validity(self):
        self.assertTrue(drivers.validate_driver(drivers.NewlineJSON))

    def test_instantiation_methods(self):
        with open(TYPES_JSON_FILE) as nlj_f, \
                open(TYPES_JSON_FILE) as drv_f, \
                drivers.NewlineJSON.from_path(TYPES_JSON_FILE) as pth_reader:
            nlj_reader = newlinejson.Reader(nlj_f)
            drv_reader = drivers.NewlineJSON(drv_f)
            for nlj, drv, pth in zip(nlj_reader, drv_reader, pth_reader):
                self.assertDictEqual(nlj, drv)
                self.assertDictEqual(nlj, pth)
                self.assertDictEqual(pth, drv)
