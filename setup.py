import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tartape",
    version="1.0.0",
    author="Leo",
    author_email="leocasti2@gmail.com",
    description="An efficient, secure, and USTAR-compatible TAR streaming engine.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/CalumRakk/tartape",
    packages=setuptools.find_packages(exclude=["tests*"]),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: System :: Archiving",
    ],
    python_requires=">=3.10.0",
    install_requires=[
        "pydantic>=2.11.7",
    ],
)
