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

Copy [.env.template](.env.template) to [.env](.env) to bootstrap the expected project environmental variables.
```sh
cp .env.template .env
```

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

## Containers

`docker` is required to run containerized version of application for local development.
