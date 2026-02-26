import setuptools

from tartape import __version__

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tartape",
    version=__version__,
    author="Leo",
    author_email="leocasti2@gmail.com",
    description="An efficient, secure, and deterministic TAR streaming engine.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/CalumRakk/tartape",
    packages=setuptools.find_packages(exclude=["tests*"]),
    include_package_data=True,
    package_data={"tartape": ["py.typed"]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: System :: Archiving",
        "Intended Audience :: Developers",
    ],
    python_requires=">=3.10.0",
    install_requires=[
        "peewee==4.0.0",
    ],
    keywords="tar, streaming, deterministic, resumable, cloud-backup, storage",
)
