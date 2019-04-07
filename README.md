# qadom

**experimental**

*peer-to-peer social network for question answering*

inspired from Quora and StackOverflow.

## Kesako?

(See below for the english version)

TODO

## What?

TODO

## How?

To get started, install the dependencies with the following command:

```shell
pip install --user pipenv
git clone https://framagit.org/amz3/qadom.git
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

Then open two browser windows:

```
xdg-open http://localhost:8000
xdg-open http://localhost:8002
```

Don't forget to refresh the page, the application doesn't use
javascript.
