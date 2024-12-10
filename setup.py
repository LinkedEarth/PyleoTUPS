from setuptools import setup, find_packages

setup(
    name = 'PyTUPS',
    version = '0.1.0',
    author = 'Deborah Khider, Jay Pujara, Dhiren Oswal',
    author_email = 'linkedearth@gmail.com',
    description = 'A package to interact with NCEI studies API',
    license = 'GPL-3.0 License',
    long_description = open('README.md').read(),
    long_description_content_type = 'text/markdown',
    url='https://github.com/LinkedEarth/PyTUPS',
    keywords = ['Paleoclimate, Data Analysis, PyTUPS'],
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
