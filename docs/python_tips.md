# Python Tips for SDK Developers

## Tip #1: Intro to Virtual Environments and uv

Everyone comes from a different perspective - please select the scenario that you most identify with.

### If you know nothing about virtual environments

If you are completely new to the concept of virtual environments, that's great! You have
nothing to "unlearn".

The uv package manager will make your life easy. It basically makes it so you
never have to worry about virtual environments. It will take care of virtual
environments for you so that you don't have to worry about dependency conflicts.

Use `uv tool install `whenever you are installing a python
program (versus a python library). It automatically creates a virtual
environment for you and automatically makes sure that the executables
contained in the python package get added to your path.

The SDK cookiecutter template already sets you up for uv. When you are
running a command with `uv run ...`, uv is doing the work to make
sure your command runs in the correct virtual environment behind the scenes.
This means you will automatically be running with whatever library versions you
have specified in `pyproject.toml` and/or with `uv add ...`. If it ever feels like
your environment may be stale, you can run `uv sync`.

### If you already know about virtual environments

Congratulations! You are ahead of the game. You probably already know enough to
start using uv.

### What is an virtual environment anyway?

The quick explanation
of a virtual environment is: a directory on your machine that holds a
full set of version-specific python packages, isolated from other copies of
those same libraries so that version conflicts from package to package
do not cause conflicts one each other. Each program can have its own version requirements
for its dependencies, and that's okay because each virtual environment is separate from
the others.

_For years, python developers have had to create, track, and manage their
virtual environments manually, but luckily, now we don't have to!_

## Tip #2: Static vs Dynamic Properties in Python and the SDK

In Python, properties within classes like Stream and Tap can generally be overridden
in two ways: _statically_ or _dynamically_. For instance, `primary_keys` and
`replication_key` should be declared statically if their values are known ahead of time
(during development), and they should be declared dynamically if they vary from one
environment to another or if they can change at runtime.

### Static example

Here's a simple example of static definitions based on the cookiecutter
[template](https://github.com/meltano/sdk/tree/main/cookiecutter/tap-template).
This example defines the primary key and replication key as fixed values which will not change.

```python
class SimpleSampleStream(Stream):
    primary_keys = ["id"]
    replication_key = None
```

### Dynamic property example

Here is a similar example except that the same properties are calculated dynamically based
on user-provided inputs:

```python
class DynamicSampleStream(Stream):
    @property
    def primary_keys(self):
        """Return primary key dynamically based on user inputs."""
        return self.config["primary_key"]

    @property
    def replication_key(self):
        """Return replication key dynamically based on user inputs."""
        result = self.config.get("replication_key")
        if not result:
            self.logger.warning("Danger: could not find replication key!")
        return result
```

Note that the first static example was more concise while this second example is more extensible.

### In summary

- Use the static syntax whenever you are dealing with stream properties that won't change
and use dynamic syntax whenever you need to calculate the stream's properties or discover them dynamically.
- For those new to Python, note that the dynamic syntax is identical to declaring a function or method, with
the one difference of having the `@property` decorator directly above the method definition. This one change
tells Python that you want to be able to access the method as a property (as in `pk = stream.primary_key`)
instead of as a callable function (as in `pk = stream.primary_key()`).

### Troubleshooting

- If you are working on a SDK tap/target that uses a `poetry-core` version before v1.0.8 in the `build-system` table of `pyproject.toml` you may have trouble specifying a `pip_url` in Meltano with "editable mode" (`-e path/to/package`) enabled
(as per [#238](https://gitlab.com/meltano/sdk/-/issues/238)). This can be resolved by upgrading
the version of `poetry-core>=1.0.8`.

For more examples, please see the [Code Samples](./code_samples.md) page.
