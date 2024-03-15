from unittest import TestCase
from v7client import config


class TestConfig(TestCase):
    
    def test_config(self):
        cfg = config.Config.build_from_env()
        print(cfg)