"""setuptools packaging."""

import setuptools

setuptools.setup(
    name="snapshot-sender-status-checker",
    version="0.0.1",
    author="DWP DataWorks",
    author_email="dataworks@digital.uc.dwp.gov.uk",
    description="Lambda for monitoring and reporting on a snapshot sender run",
    long_description="Lambda for monitoring and reporting on a snapshot sender run",
    long_description_content_type="text/markdown",
    entry_points={"console_scripts": ["status_checker=status_checker:main"]},
    package_dir={"": "src"},
    packages=setuptools.find_packages("src"),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
