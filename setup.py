from setuptools import setup, find_packages

setup(
    name="segadb",
    version="0.1.0",
    packages=find_packages(),
    author="Santiago Gonzalez",
    author_email="sega97@gmail.com",
    description="A simple database library built in Python",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url="https://github.com/SantiagoEnriqueGA/custom_database",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    entry_points={
        'console_scripts': [
            'segadb_cli=segadb.segadb_cli:app',
        ],
    },
)
