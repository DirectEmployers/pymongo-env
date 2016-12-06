import os
from setuptools import setup, find_packages


with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as readme:
    README = readme.read()


setup(
    name='pymongo-env',
    version='0.0.1',

    description='An overlay for MongoDB that allows for easy environment switching.',
    long_description=README,

    packages=find_packages(exclude=('example', )),
    include_package_data=True,
    install_requires=[
    ],

    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],

    zip_safe=False,
)