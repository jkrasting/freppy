"""
conftest.py : test configuration file
"""

import os
import pytest
import tarfile
import tempfile
import shutil

import pkg_resources as pkgr

def pytest_sessionstart(session):
    pytest.wkdir = tempfile.mkdtemp()

    _ppdir = f"{pytest.wkdir}/GFDL-experiment/gfdl.intel18-prod-openmp"
    os.makedirs(_ppdir)

    tfile = pkgr.resource_filename("freppy","resources/small_pp_tree.tar.gz")
    tfile = tarfile.open(tfile)
    tfile.extractall(_ppdir)
    tfile.close()

    pytest.ppdir = f"{_ppdir}/pp"


def pytest_sessionfinish(session, exitstatus):
    shutil.rmtree(pytest.wkdir)