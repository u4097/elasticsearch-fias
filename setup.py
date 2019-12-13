import setuptools

# read the contents of your README file
from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

<<<<<<< HEAD
with open ("README.rst", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="fias-elasticsearch",
    version="1.4.0",
=======

setuptools.setup(
    name="fias-elasticsearch",
    version="1.4.5",
>>>>>>> feature/setup
    author="Oleg Sitnikov",
    author_email="oleg.sitnikov@icloud.com",
    description="Импорт базы данных ФИАС в формате XML в базу Еlasticsearch, с возможностью последующего обновления",
    long_description=long_description,
<<<<<<< HEAD
=======
    long_description_content_type='text/markdown',
>>>>>>> feature/setup
    url="https://github.com/u4097/elasticsearch-fias",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Topic :: Database',
        'Topic :: Software Development',
        'Natural Language :: Russian'
    ],
    python_requires='>=3.6',
)
