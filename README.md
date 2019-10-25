# pynats2

Thread based python client for NATS messaging system.

This project is a replacement for abandoned [pynats](https://github.com/mcuadros/pynats) and [nats-python](git@github.com:Gr1N/nats-python). `pynats2` supports only Python 3.6+ and fully covered with typings.

Go to the [asyncio-nats](https://github.com/nats-io/asyncio-nats) project, if you're looking for `asyncio` implementation.

## Installation

```sh
$ pip install pynats2
```

## Usage

```python
from pynats2 import NATSClient

with NATSClient() as client:
    client.publish("test-subject", payload=b"test-payload")
```

## Contributing

To work on the `pynats2` codebase, you'll want to clone the project locally and install the required dependencies via pip.

```sh
$ git clone git@github.com:yosshy/pynats2.git
$ pip install -r requirements.txt
$ python setup.py install
```

To run tests and linters use command below:

```sh
$ black --check --diff .
$ flake8 pynats2 tests
$ mypy --ignore-missing-imports --follow-imports=silent pynats2 tests
```

## License

`pynats2` is licensed under the MIT license. See the license file for details.
