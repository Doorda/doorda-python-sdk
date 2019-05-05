import doordahost_glue

import os
try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

    def find_packages(where='.'):
        return [folder.replace("/", ".").lstrip(".")
                for (folder, _, fils) in os.walk(where)
                if "__init__.py" in fils]


INSTALL_REQUIRES = ["doorda_sdk>=1.0.8", "requests>=2.21.0"]

setup(
    name="doordahost_etl",
    version=doordahost_glue.__version__,
    packages=["doordahost_glue"] + ['doordahost_glue.' + i for i in find_packages('doordahost_glue')],
    install_requires=INSTALL_REQUIRES
)
