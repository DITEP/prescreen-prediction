from setuptools import setup, find_packages
from codecs import open
from os import path


here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


setup(
    name='prescreen-prediction',

    version='0.1',

    description='Screen Failures prediction based on Electronic Health '
                'Records ',

    long_description=long_description,

    author='Valentin Charvet',

    author_email='valentin.charvet@gustaveroussy.fr',

    packages=find_packages()
    )
