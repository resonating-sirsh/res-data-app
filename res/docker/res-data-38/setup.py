from setuptools import setup, find_packages

requirements = []
with open("requirements.txt") as f:
    requirements = f.read().splitlines()

setup(
    name="res",
    version="0.1.0",
    install_requires=requirements,
    description="res-data-platform library for data ingestion and processing",
    packages=find_packages(),
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
    entry_points={"console_scripts": ["res = res.__main__:app"]},
    classifiers=[
        "Development Status :: Beta",
        "Programming Language :: Python :: 3.7",
    ],
)
