# Frequently Asked Questions

## What if I select the wrong option in the cookiecutter prompt?

Most likely you should delete the project and start over.

## What are all of the Property options?

The property types are documented in the [JSON Schema builder docs](./typing).
However, if you're using an IDE such as VSCode, you should be able to set up the environment to give you autocompletion prompts or hints.
Ensure your interpreter is set to poetry if you've followed the [Dev Guide](./dev_guide.md).
Checkout this [gif](https://visualstudiomagazine.com/articles/2021/04/20/~/media/ECG/visualstudiomagazine/Images/2021/04/poetry.ashx) for how to change your interpreter.

## I'm having trouble getting the base class to __init__.

Ensure you're using the `super()` method to inherit methods from the base class.

```python
class MyStream(Stream):
    """Mystream stream class."""
    def __init__(self, tap: Tap):
        super().__init__(tap)
        self.conn...
```
