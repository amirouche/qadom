help: ## This help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort


dev: # wiredtiger ## Prepare the ubuntu host sytem for development
	pip3 install pipenv --user --upgrade || pip3 install pipenv --upgrade
#	PYENV_ROOT=$(PWD)/submodules/pyenv PATH=$(PWD)/submodules/pyenv/bin:$(HOME)/.local/bin:$(PATH) pipenv install --dev --skip-lock
#	pipenv run python setup.py develop
	pipenv run pre-commit install --hook-type pre-push

devrun:  ## Run the web app
	PYTHONPATH=$(PWD) DEBUG=DEBUG adev runserver --livereload qadom/web.py

check: ## Run tests
	PYTHONHASHSEED=0 PYTHONPATH=$(PWD) pipenv run pytest -vvv --cov-config .coveragerc --cov-report html --cov-report xml --cov=qadom -s .
	# pipenv check  TODO: uncomment but check travis ci
	pipenv run bandit --skip=B101 qadom/ -r
	@echo "\033[95m\n\nYou may now run 'make lint'.\n\033[0m"

lint: ## Lint the code
	pipenv run pylama qadom/ | grep -v E501

clean: ## Clean up
	git clean -fXd

todo: ## Things that should be done
	@grep -nR --color=always TODO qadom/

xxx: ## Things that require attention
	@grep -nR --color=always --before-context=2  --after-context=2 XXX qadom/

publish: check ## Publish to pypi.org
	pipenv run python setup.py sdist upload
