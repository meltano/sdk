# Singer Tap Porting Guide

This guide walks you through the process of migrating an existing Singer Tap over to the SDK.

_Want to follow along in a real world porting example? See our recorded pair coding session for the `tap-gitlab` port [here](http://www.youtube.com/watch?v=XNm5BN_zluw)._

## A Clear Slate

When porting over an existing tap, most developers find it easier to start from a fresh repo than to incrementally change their existing one.

1. Within your existing repo, create a new branch.
1. Move _all_ of the files in the old branch into a subfolder called "archive".
1. Commit and push the result to your new branch. (You'll do this several times along the way, which creates a fresh tree and a fresh diff for subsequent commits.)
1. Now follow the steps in the [dev guide](../dev_guide.md#building-a-new-tap-or-target) to create a new project using the Tap cookiecutter.
1. Copy all the files from the cookiecutter output into your main repo and commit the result.

## Settings and Readme

Next, we'll copy over the settings and readme from the old project to the new one.

1. Open `archive/README.md` and copy-paste the old file into the new `README.md`. Generally, the best place to insert it is in the settings section, then move things around as needed. Commit the result when you're happy with the new file.
2. Open the `README.md` in side-by-side mode with `tap.py` and copy paste each setting and it's description into an appropriate type helper class (prefixed with `th.*`).
   - If settings are not defined in your README.md, you can try searching the `archived/**.py` files for references to `config.get(` or `config[`, and/or check any other available reference for the expected input settings.

## If you are building a SQL tap

Since SQL taps leverage the excellent SQLAlchemy library, most behaviors are already predefined and automatic. This includes catalog creation, table scanning, and many other common challenges.

If you are porting over a SQL tap... skip ahead now to the [Installing Dependencies](#install-dependencies) and make sure your SQL provider's SQLAlchemy drivers are included in the added library dependencies. Also, when you get the step of searching for TODO items, pay close attention to `get_sqlalchemy_url()` since this will drive authentication and connectivity.

Continue until just before you reach the "Pagination" section, at which point you are probably done! 🚀 Optionally, you can further optimize performance by overriding `get_records()` with a sync method native to your SQL operations.

## Authentication

1. Open the `auth.py` or `client.py` file (depending on auth method) and locate the authentication logic.
2. In your archived files, open `client.py` or whichever file pertains to authentication. (You'll use this for reference in the next step.)
3. Update the authenticator methods by applying the logic and config values as demonstrated in the archived python code.

## Define your first stream

Before you begin this section, please select a stream you would like to port as your first stream. This should be a simple stream without complex logic. If your tap has nested structure, start with a top level stream rather than a child stream.

1. Open `streams.py` and modify one of the samples to fit your first stream's name.
2. Make sure you set `primary_keys`, `replication_key` first.
3. If you have a schema file for each stream:
   1. Move the entire `schemas` folder out of the `archive` directory.
   2. Set `schema_filepath` property to be equal to the schema file for this stream.
4. If you are declaring schemas directly (without an existing JSON schema file):
   1. Using the typing helpers (`th.*`) to define just the `primary_key`, `replication_key`, and 3-5 additional fields.
   2. Don't worry about defining all properties up front. Instead, come back to this step _after_ you finish a successful stream test.

Once you have a single stream defined, with 3-6 properties, you're ready to continue to the next step.

## Install dependencies

Check for required libraries in `archive/requirements.txt` and/or `archive/setup.py`. For each library you find:

````{tab} Poetry
```console
# Add a library:
poetry add my-library

# Or add the same library with version constraints:
poetry add my-library==1.0.2
```
````

````{tab} uv
```console
# Add a library:
uv add my-library

# Or add the same library with version constraints:
uv add my-library==1.0.2
```
````

Note:

- You can probably skip any libraries related to `requests`, `backoff`, or `singer-*` - as these functions are already managed in the SDK.

Once you have the necessary dependencies added, run `poetry install`/`uv sync` to make sure everything is ready to go.

## Perform `TODO` items in `tap.py` and `client.py`

1. Open `tap.py` and search for TODO items. Depending on the type of tap you are porting, you will likely have to provide your new stream's class names so that the tap class knows to invoke them.
2. Open `client.py` and search for TODO items. If your API type requires a `url_base`, go ahead and input it now.
   - You can postpone the other TODOs for now. Pagination will be addressed in the steps below.

Note: You _do not_ have to resolve TODOs everywhere in the project, but if there are any sections you can obviously resolve, you can go ahead and do so now.

## Test, debug, repeat, ... until... _success!_

This is the stage where you'll finally see data streaming from the tap. 🙌

If you have not already done so, run `poetry install`/`uv` to make sure your project and its dependencies are properly installed in your virtual environment.

Repeat the following steps until you see a help message:

````{tab} Poetry
1. Run `poetry run tap-mysource --help` to confirm the program can run.
2. Find and fix any errors that occur.
````

````{tab} uv
1. Run `uv run tap-mysource --help` to confirm the program can run.
2. Find and fix any errors that occur.
````

Now, repeat the following steps until you get data coming through your tap:

````{tab} Poetry
1. Run `poetry run tap-mysource` to attempt your first data sync.
2. Find and fix any errors that occur.
````

````{tab} uv
1. Run `uv run tap-mysource` to attempt your first data sync.
2. Find and fix any errors that occur.
````

If you run into error, go back and debug, and especially double check your authentication process and input credentials.

If you're able to see data coming from your tap, **congrats!**

_Important: If you've gotten this far, this is a good time to commit your code back to your branch. In case anything breaks in the subsequent steps, you'll easily be able to get back to this point and/or see what has changed since the successful sync._

## Define pagination

Pagination is generally unique for almost every API. There's no single method that solves for very different API's approach to pagination.

Most likely you will use [get_new_paginator](singer_sdk.RESTStream.get_new_paginator) to instantiate a [pagination class](./../classes/singer_sdk.pagination.BaseAPIPaginator.rst) for your source, and you'll use `get_url_params` to define how to pass the "next page" token back to the API when asking for subsequent pages.

When you think you have it right, run `poetry run tap-mysource`/`uv run tap-mysource` again, and debug until you are confident the result is including multiple pages back from the API.

You can also add unit tests for your pagination implementation for additional confidence:

```python
from singer_sdk.pagination import BaseHATEOASPaginator, first


class CustomHATEOASPaginator(BaseHATEOASPaginator):
   """Paginator for HATEOAS APIs - or "Hypermedia as the Engine of Application State".

   This paginator expects responses to have a key "next" with a value
   like "https://api.com/link/to/next-item".
   """"

   def get_next_url(self, response: Response) -> str | None:
      """Get a parsed HATEOAS link for the next, if the response has one."""

      try:
            return first(
               extract_jsonpath("$.links[?(@.rel=='next')].href", response.json())
            )
      except StopIteration:
            return None


def test_paginator_custom_hateoas():
   """Validate paginator that my custom paginator."""

   resource_path = "/path/to/resource"
   response = Response()
   paginator = CustomHATEOASPaginator()
   assert not paginator.finished
   assert paginator.current_value is None
   assert paginator.count == 0

   response._content = json.dumps(
      {
         "links": [
               {
                  "rel": "next",
                  "href": f"{resource_path}?page=2&limit=100",
               }
         ]
      }
   ).encode()
   paginator.advance(response)
   assert not paginator.finished
   assert paginator.current_value.path == resource_path
   assert paginator.current_value.query == "page=2&limit=100"
   assert paginator.count == 1

   response._content = json.dumps(
      {
         "links": [
               {
                  "rel": "next",
                  "href": f"{resource_path}?page=3&limit=100",
               }
         ]
      }
   ).encode()
   paginator.advance(response)
   assert not paginator.finished
   assert paginator.current_value.path == resource_path
   assert paginator.current_value.query == "page=3&limit=100"
   assert paginator.count == 2

   response._content = json.dumps({"links": []}).encode()
   paginator.advance(response)
   assert paginator.finished
   assert paginator.count == 3
```

Note: Depending on how well the API is designed, this could take 5 minutes or multiple hours. If you need help, sometimes [PostMan](https://postman.com) or [Thunder Client](https://marketplace.visualstudio.com/items?itemName=rangav.vscode-thunder-client) can be helpful in debugging the APIs specific quirks.

## Run pytest

Now is a good time to test that the built-in tests are working as expected:

````{tab} Poetry
```console
poetry run pytest
```
````

````{tab} uv
```console
uv run pytest
```
````

## Create the remaining streams

Now that basic authentication, pagination, and test are all working, you can freely add the remaining streams to `streams.py`.

Notes:

- As should be expected, you are free to subclass streams in order to have their behavior be inherited from other stream classes.
  - For instance, if 3 streams use one pagination method, and 5 other streams use a different method, you can have each stream created as a subclass of a stream that has desired behavior.
- If you have streams which invoke each other in a nested layout, please refer to the `parent_stream_class` property and its [related documentation](../stream_maps.md).
- As before, if you do not already have a full JSON Schema file for each stream type, it is generally a good practice to start with just 5-8 properties per stream. You don't have to define all properties up front and before doing so, it is generally more valuable to test that each stream is getting data.

## Run pytest again, add stream properties, and repeat

Now that all streams are defined, run `pytest` again.

````{tab} Poetry
```console
poetry run pytest
```
````

````{tab} uv
```console
uv run pytest
```
````

1. If pytest is successful, add properties missing from your prior iteration.
2. Run pytest again.
3. Continue adding properties and testing until all streams are fully defined.

## Optional Next Steps

### Handle legacy state conversions

The SDK will automatically handle `STATE` for you 99% of the time. However, it is very likely that the legacy version of the tap has a different `STATE` format in comparison with the SDK format. If you want to seamlessly support both old and new STATE formats, you'll need to define a conversion operation.

To handle the conversion operation, you'll override [`Tap.load_state()`](singer_sdk.Tap.load_state). The exact process of converting state is outside of this guide, but please check the [STATE implementation docs](../implementation/state.md) for an explanation of general format expectations.

### Leverage Auto Generated README

The SDK provides autogenerated markdown you can paste into your README:

````{tab} Poetry
```console
poetry run tap-mysource --about --format=markdown
```
````

````{tab} uv
```console
uv run tap-mysource --about --format=markdown
```
````

This text will automatically document all settings, including setting descriptions. Optionally, paste this into your existing `README.md` file.
