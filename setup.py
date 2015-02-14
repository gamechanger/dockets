import os
import dockets
import setuptools

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setuptools.setup(
    name="Dockets",
    version=dockets.__version__,
    author="Doug Woos",
    author_email="doug@gamechanger.io",
    description="SUPER simple Redis-backed queueing in Python",
    license="BSD",
    keywords="redis queue schedule",
    url="http://github.com/gamechanger/dockets",
    packages=["dockets"],
    package_data={"dockets": ["lua/*.lua"]},
    long_description="LONG",
    install_requires=['simplejson','redis>=2.10','python-dateutil'],
    test_suite = 'nose.collector',
    tests_require=['nose', 'mock']
)
