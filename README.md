# qadom

**still alpha**

**peer-to-peer social network for question answering**

inspired from Quora and StackOverflow.

## Getting started

To get started, install the dependencies with the following command:

```shell
pip install --user pipenv
git clone https://github.com/amirouche/qadom
cd qadom
pipenv shell
pipenv install --dev
```

Then in a first terminal run:

```shell
pipenv run make devrun
```

In other terminal

```shell
pipenv run make devrun2
```

Then open two firefox windows:

```
firefox http://localhost:8000
firefox http://localhost:8002
```

Don't forget to refresh the page, the application doesn't use
javascript.
