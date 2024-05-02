import setuptools

setuptools.setup(
    name="data-pipeline",
    version="0.0.1",
    packages=setuptools.find_packages(),
    install_requires=["apache-beam[gcp]==2.56.0", "google-cloud-firestore==2.14.0"],
    package_data={"schema": ["*.json"]},
)
