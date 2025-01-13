FROM python:3.13.1-slim

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1
# Supress warning that pip running as root user, OK for container
ENV PIP_ROOT_USER_ACTION=ignore
# UTF Language Support
ENV LANG=C.UTF-8

# Install required project packages
WORKDIR /app/
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --no-compile -r requirements.txt && rm requirements.txt

# Install odin package
COPY pyproject.toml pyproject.toml
COPY src/ src/
RUN pip install . && rm -r build

CMD ["start-odin"]
