# Use the fully qualified Miniconda3 image from Docker Hub.
FROM docker.io/continuumio/miniconda3

# Install system build dependencies required by some pip packages.
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python 3.11, tomopy, dxchange, and matplotlib from conda-forge.
RUN conda install -y -c conda-forge python=3.11 pytorch libmagic matplotlib && \
    conda clean -afy

# Upgrade pip and build tools.
RUN pip install --upgrade pip setuptools wheel

# Install globus-compute-sdk via pip.
RUN pip install --no-cache-dir globus-compute-sdk

# Install boost_corr package in editable mode.
RUN pip install -e git+https://github.com/AZjk/boost_corr#egg=boost_corr

# Default command: launch an interactive Python shell.
CMD ["python3"]

# # Use the fully qualified CUDA image from Docker Hub.
# FROM docker.io/nvidia/cuda:12.8.0-runtime-ubuntu24.04

# # Install system dependencies needed for downloading and installing Miniconda.
# RUN apt-get update && apt-get install -y wget bzip2 git && rm -rf /var/lib/apt/lists/*

# # Install Miniconda.
# RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O /tmp/miniconda.sh && \
#     bash /tmp/miniconda.sh -b -p /opt/conda && \
#     rm /tmp/miniconda.sh
# # Update PATH so that conda and its python are available.
# ENV PATH=/opt/conda/bin:$PATH

# # Install Python 3.11, tomopy, dxchange, matplotlib, pytorch and cudatoolkit from conda-forge.
# RUN conda install -y -c conda-forge python=3.11 pytorch cudatoolkit && \
#     conda clean -afy

# # Install globus-compute-sdk via pip.
# RUN pip install --no-cache-dir globus-compute-sdk

# # Install boost_corr package in editable mode from the nexus-support branch.
# RUN pip install -e "git+https://github.com/AZjk/boost_corr@nexus-support#egg=boost_corr"

# # Default command: launch an interactive Python shell.
# CMD ["python3"]