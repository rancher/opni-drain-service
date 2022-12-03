## DRAIN service

* This service controls the training and retraining of all other ML/DL models by sending a NATS signal after volatility of log templates assigned to incoming logs has decreased to an acceptable level
* Adopted from https://github.com/IBM/Drain3 and https://pinjiahe.github.io/papers/ICWS17.pdf

## Testing
pip install requirement tools
```
pip install -r requirements.txt
pip install -r test-requirements.txt
```

run pytest and check the coverage:
```
pytest --cov
```

## Contributing
We use `pre-commit` for formatting auto-linting and checking import. Please refer to [installation](https://pre-commit.com/#installation) to install the pre-commit or run `pip install pre-commit`. Then you can activate it for this repo. Once it's activated, it will lint and format the code when you make a git commit. It makes changes in place. If the code is modified during the reformatting, it needs to be staged manually.

```
# Install
pip install pre-commit

# Install the git commit hook to invoke automatically every time you do "git commit"
pre-commit install

# (Optional)Manually run against all files
pre-commit run --all-files
```
