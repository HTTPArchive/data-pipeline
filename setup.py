import setuptools

setuptools.setup(
    name="data-pipeline",
    version="0.0.1",
    packages=setuptools.find_packages(),
    install_requires=["apache-beam[gcp]==2.40.0"],
    package_data={"schema": ["*.json"]},
)
