#!/usr/bin/env python3

import unittest
from copy import deepcopy
from unittest import mock
from status_checker_lambda.status_checker import *


class TestReplayer(unittest.TestCase):
    def test_initial_test(self):
        assert True