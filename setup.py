""" setup script """
import setuptools

exec(open("freppy/version.py").read())

setuptools.setup(version=__version__)
