# `tap-base` - an open framework for building Singer-compliant taps

- _Note: This framework is still in early development and planning phases_

## Strategies for Optimized Tap Development

1. **Universal Code Formatting.**
    - _From the [Black](https://black.readthedocs.io) product description:_
      > By using Black, you agree to cede control over minutiae of hand-formatting. In return, Black gives you speed, determinism, and freedom from pycodestyle nagging about formatting. You will save time and mental energy for more important matters. **Black makes code review faster by producing the smallest diffs possible.** Blackened code looks the same regardless of the project you’re reading. **Formatting becomes transparent after a while and you can focus on the content instead.**
    - _If you use our companion cookiecutter template, your project will start out auto-formatted by Black. You can keep this default or change it - the choice is yours._
2. **Pervasive Python Type Hints.**
    - _Spend less time reading documentation due to pervasive type declarations in our base class._
3. **Less is More.**
    - _Because taps build from the template require less code and taking advantage of a common set of  base class capabilities, developers can dramatically reduce the time to develop a fully mature tap._
4. **Create Future-Proof Plugins.**
    - _Take advantage of new base class capabilities by simply updating your dependency version and retesting with the latest version._

## Tap Dev Guide

See the [dev guide](docs/dev_guide.md) for instructions on how to get started building your own
taps.
