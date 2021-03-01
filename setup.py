"""setuptools packaging."""

import setuptools

setuptools.setup(
    name="pdm-emr-launcher-handler",
    version="0.0.1",
    author="DWP DataWorks",
    author_email="dataworks@digital.uc.dwp.gov.uk",
    description="A lambda that deals with launching PDM cluster",
    long_description="A lambda that deals with launching PDM cluster based on CloudWatch event",
    long_description_content_type="text/markdown",
    entry_points={
        "console_scripts": ["pdm_emr_launcher_handler=pdm_emr_launcher_handler:handler"]
    },
    package_dir={"": "src"},
    packages=setuptools.find_packages("src"),
    install_requires=["argparse", "boto3", "moto"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
