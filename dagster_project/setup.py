from setuptools import find_packages, setup

setup(
    name="dagster_project",
    packages=find_packages(exclude=["dagster_project_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "requests",
        "duckdb",
        "boto3",
        "dlt[duckdb, filesystem]",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
