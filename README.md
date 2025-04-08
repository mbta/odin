# odin

MBTA application responsible for ingesting and aggregating fares related data. 

# Developer Usage

## Dependencies

[asdf](https://asdf-vm.com/) is used to mange runtime versions using the command line. Once installed, run the following in the root project directory:

```sh
# add project plugins
asdf plugin add python
asdf plugin add direnv

# install versions of plugins specified in .tool-versions
asdf install
```

Follow [instructions](https://direnv.net/) for hooking `direnv` into your shell. 

## Creating Local Environment

`direnv` manages the loading/unloading of environmental variables, as well as the creation & activation of a local python virtual environment for development. Using `direnv`, whenever a shell moves into the project directory, appropriate environmental variables will be loaded automagically and the python virtual environment will be created/activated.

After `direnv` is properly hooked, allow in project:
```sh
direnv allow
```

`direnv - python layout` combined with `pip-tools` is used by to manage python dependencies and the local python virtual environment. Python package dependencies are defined in [pyproject.toml](pyproject.toml). 

Run the [config_venv.sh](config_venv.sh) script after a repository clone, or after changing [pyproject.toml](pyproject.toml) dependencies.
```sh
# script updates `requirements` files and syncs packages to local python virtual environment.
./config_venv.sh
```

## Job Configuration

Which jobs to run, and their configuration, is controlled by a file `config.toml`. (It can also be passed in as an environment variable `ODIN_CONFIG`).

Copy `config.toml.template` to `config.toml`, then uncomment the jobs you want to be started when the application starts.

## Running the application.

For local development, it's recommended to use `docker` to run  a containerized version of the application.

It's also possible to run the run the application with `python -c 'from odin.run import start; start()'`.

Take care, it is possible for a local version of the application to modify files in s3, if you have permissions and the application is configured that way.

- Run tests with `pytest tests`
- Run the formatter with `ruff format .`
