# alertbase
This is an experimental database for ZTF's alerts.

## Developing

To start, you'll need Python 3.8.

Clone the repository.

Install [`git-lfs`](https://git-lfs.github.com/) so that you can download test
datasets. Run `git lfs install`, and then `git lfs checkout`.

Run `make dev-setup`.

You're good to go! Verify the installation worked with `make test`.
