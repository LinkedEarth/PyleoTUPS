from setuptools import setup, find_packages

setup(
    name = 'pytups',
    version = '0.0.1',
    author = 'Deborah Khider, Jay Pujara, Dhiren Oswal',
    author_email = 'linkedearth@gmail.com',
    description = 'A package to interact with NCEI studies API',
    license = 'Apache License 2.0',
    long_description = open('README.md').read(),
    long_description_content_type = 'text/markdown',
    url='https://github.com/LinkedEarth/PyTUPS/pytups',
    keywords = ['Paleoclimate, Data Analysis, Table Understanding'],
    packages = find_packages(),
    install_requires = [
        'pandas',
        'requests',
        'numpy',
        'pybtex'  
    ],
    classifiers = [],
    python_requires = '>=3.7',
)
