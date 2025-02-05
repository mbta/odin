FROM python:3.13.1-slim

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1
# Supress warning that pip running as root user, OK for container
ENV PIP_ROOT_USER_ACTION=ignore
# Supress pip new release warning
ENV PIP_DISABLE_PIP_VERSION_CHECK=1
# UTF Language Support
ENV LANG=C.UTF-8

# Install required project packages
WORKDIR /install/
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --no-compile -r requirements.txt && rm requirements.txt

# Install odin package
COPY pyproject.toml pyproject.toml
COPY src/ src/
RUN pip install . && rm -r build

# Create/Use non-root container USER
RUN groupadd -g 1000 odin && \
    useradd -m -u 1000 -g odin odin
USER odin

WORKDIR /runner/
