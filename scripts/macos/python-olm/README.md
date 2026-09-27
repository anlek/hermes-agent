# Matrix encryption on macOS

`mautrix[encryption]` requires `python-olm` 3.2.16. Its PyPI sdist fails with
current Apple clang and CMake, so the fork's `matrix` extra uses a patched sdist
on macOS. Linux continues to use the PyPI release. Windows remains unsupported.

The [fork release](https://github.com/anlek/hermes-agent/releases/tag/python-olm-3.2.16-macos.1)
contains the patched [sdist](https://github.com/anlek/hermes-agent/releases/download/python-olm-3.2.16-macos.1/python-olm-3.2.16%2Bmacos.tar.gz)
and a Python 3.14 arm64 wheel. The sdist SHA-256 is
`025ac79a3af6cd69d370f25405c7bd436a6c5feaa816d7b8942394297289e0ee`.
The local patch is `python-olm-3.2.16-macos-clang.patch`.

For a new Python version, extract the release sdist and build a matching wheel
from its root:

```sh
uv build --wheel --python <py> --out-dir out .
```

When updating `python-olm`, apply and verify the patch against the new sdist,
publish a new release asset, update the URL in `pyproject.toml`, and run
`uv lock`.
