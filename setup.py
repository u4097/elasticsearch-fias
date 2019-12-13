import setuptools


with open ("README.rst", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="fias-elasticsearch",
    version="1.4.0",
    author="Oleg Sitnikov",
    author_email="oleg.sitnikov@icloud.com",
    description="Download fias XML base, import it in Elasticsearch, and update",
    long_description=long_description,
    url="https://github.com/u4097/elasticsearch-fias",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    python_requires='>=3.6',
)
