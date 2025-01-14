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

`pip-tools` is used by to manage python dependencies. 

> `direnv` will automatically create (and activate) a python virutal environemnt in the project folder for local development. `pip-tools` is also automatically run (by `direnv`) to install project dependencies when moving into the project directory.
>
> `pip-tools` creates [requirements.txt](requirements.txt) and [requirements-dev.txt](requirements-dev.txt) files containing versions and hashes of all python library dependencies for the project. 

`docker` is required to run containerized version of application for local development.

## Environmental Variables

Project environmental variables are stored in [.env](.env) and managed for command line usage with `direnv`.

Using `direnv`, whenever a shell moves into any project directory, the environmental variables defined in [.env](.env) are loaded automagically. 
