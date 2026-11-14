"""Explicit offline suite only; first module installs audit/import guards."""
import importlib.util
from pathlib import Path
import sys
import unittest
root=Path(__file__).resolve().parents[2]
paths=('validation/result_download/test_download.py','validation/text_registration/test_authority_preflight.py')
suite=unittest.TestSuite()
for i,path in enumerate(paths):
    spec=importlib.util.spec_from_file_location('download_offline_'+str(i),root/path)
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
    suite.addTests(unittest.defaultTestLoader.loadTestsFromModule(module))
result=unittest.TextTestRunner(verbosity=2).run(suite)
raise SystemExit(not result.wasSuccessful())
