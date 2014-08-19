import os
import setuptools

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setuptools.setup(
    name="Dockets",
    version="0.1.0",
    author="Doug Woos",
    author_email="doug@gamechanger.io",
    description="SUPER simple Redis-backed queueing in Python",
    license="BSD",
    keywords="redis queue schedule",
    url="http://github.com/gamechanger/dockets",
    packages=["dockets"],
    long_description="LONG",
    install_requires=['simplejson','redis','python-dateutil'],
    test_suite = 'nose.collector',
    tests_require=['nose', 'mock']
)
