from setuptools import setup

setup(
    name='arcaflow',
    version='0.0.0',
    license_files = ('LICENSE',),
    package_data={
        'arcaflow':['bin/arcaflow*']
    },
    description="Arcaflow engine python wrapper",
    author="Arcalot Contributors",
    license="Apache",
    packages=['arcaflow'],
    url="https://arcalot.io/arcaflow/"
)