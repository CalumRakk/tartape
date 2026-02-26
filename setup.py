import setuptools

def get_version():
    with open("tartape/__init__.py") as f:
        for line in f:
            if line.startswith("__version__"):
                return line.split("=")[1].strip().strip('"')

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tartape",
    version=get_version(),
    author="Leo",
    author_email="leocasti2@gmail.com",
    description="An efficient, secure, and deterministic TAR streaming engine.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/CalumRakk/tartape",
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
    packages=setuptools.find_packages(),
    python_requires=">=3.10.0",
    install_requires=[
        "peewee==4.0.0",
    ],
    keywords="tar, streaming, deterministic, resumable, cloud-backup, storage",
)
