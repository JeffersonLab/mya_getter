from setuptools import setup, find_packages
import codecs
import os.path
import pathlib

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / "README.md").read_text(encoding="utf-8")

def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()


def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")


setup(
    name='mya_getter',
    version=get_version("src/mya_getter/__init__.py"),
    author='Adam Carpenter',
    author_email="adamc@jlab.org",
    description="A python wrapper on standard CEBAF MYA utilities",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(where='src'),
    package_dir={"": "src"},
    install_requires=[
        'tqdm',
        'pandas',
        'requests',
    ],
    extra_requires={
        'interactive': ['matplotlib']
    }
)
