# Testing for performance in your package

We've had success using [`viztracer`](https://github.com/gaogaotiantian/viztracer) to create flame graphs for SDK-based packages and find if there are any serious performance bottlenecks.

## Use `viztracer` as a dev dependency

You can start doing that in your package with

```console
$ poetry add --dev viztracer
```

Then simply run your package's CLI as normal, preceded by the `viztracer` command

```console
$ poetry run viztracer my-tap
```

That command will produce a `result.json` file which you can explore with the `vizviewer` tool. 

```console
$ poetry run vizviewer result.json
```

Thet output should look like this

![SDK Flame Graph](https://gitlab.com/meltano/sdk/uploads/07633ba1217de6eb1bb0e018133c608d/_write_record_message.png)

**Note**: Chrome seems to work best for running the `vizviewer` app.
