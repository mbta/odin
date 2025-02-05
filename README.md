# odin

MBTA application responsible for ingesting and aggregating fares related data. 

# Developer Usage

## Dependencies

[asdf](https://asdf-vm.com/) is used to mange runtime versions using the command line. Once installed, run the following in the root project directory:

```sh
# add project plugins
asdf plugin-add python
asdf plugin-add direnv

# install versions of plugins specified in .tool-versions
asdf install
```

### Local Environment

`direnv` manages the loading/unloading of environmental variables, as well as the creation of a python virtual environment for local development. Using `direnv`, whenever a shell moves into the project directory, appropriate environmental variables will be loaded automagically and the python virtual environment will be activated.

> copy [.env.template](.env.template) to [.env](.env) to bootstrap the expected project environmental variables.


`direnv - python layout` combined with `pip-tools` is used by to manage python dependencies and the local python virtual environment. 

> Python package dependencies are defined in [pyproject.toml](pyproject.toml). 
>
> Run `./config_venv.sh` after a repository clone, or after changing [pyproject.toml](pyproject.toml) dependencies. The [config_venv.sh](config_venv.sh) script will update `requirements` files and sync packages to local python virtual environment.

### Containers

`docker` is required to run containerized version of application for local development.
