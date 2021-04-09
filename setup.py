import os
from setuptools import setup, find_packages


install_requires = []
with open('requirements.txt') as reqs:
    for line in reqs.readlines():
        req = line.strip()
        if not req or req.startswith('#'):
            continue
        install_requires.append(req)

setup(
    name='gladier_xpcs',
    description='The XPCS Gladier',
    url='https://github.com/globus-gladier/gladier_xpcs',
    maintainer='The Gladier Team',
    maintainer_email='',
    version='0.0.1',
    packages=find_packages(),
    install_requires=install_requires,
    dependency_links=[],
    license='Apache 2.0',
    classifiers=[]
)
