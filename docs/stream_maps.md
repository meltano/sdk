# Inline Stream Maps

## Introduction

SDK-based taps, targets, and mappers automatically support the custom inline mappings feature. Stream mappings can be applied to solve the following real-world applications:

### Note on `null` values

In all examples below where `null` is used as a value, the special string `"__NULL__"` can be used instead.

### Stream-Level Mapping Applications

- **Stream aliasing:** streams can be aliased to provide custom naming downstream.
- **Stream filtering:** streams records can be filtered based on any user-defined logic.
- **Stream duplication:** streams can be split or duplicated and then sent as multiple distinct
  streams to the downstream target.

### Property-Level Mapping Applications

- **Property-level aliasing:** properties can be renamed in the resulting stream.
- **Property-level transformations:** properties can be transformed inline.
- **Property-level exclusions:** properties can be removed from the resulting stream.
- **Property-level additions:** new properties can be created based on inline user-defined
  expressions.

### Schema Flattening Applications

- ***Flatten nested properties:** separates large complex properties into multiple distinct fields.

For instance, a complex `user` property may look like this:

```js
{
    // ...
    "user": {
        "first_name": "Jane",
        "last_name": "Carter",
        "id": "jcarter"
    }
}
```

Rather than receive the entire record as one large structure, flattening the record would output
three distinct fields:

- `user__first_name`
- `user__last_name`
- `user__id`

#### Flattening Example

````{tab} meltano.yml
```yaml
flattening_enabled: true
flattening_max_depth: 1   # flatten only top-level properties
```
````

````{tab} JSON
```json
{
  "flattening_enabled": true,
  "flattening_max_depth": 1
}
```
````

## Out-of-scope capabilities

These capabilities are all out of scope _by design_:

- **Mappers do not support aggregation.**
  - To aggregate data, first land the data and then apply aggregations using a
    transformation tool like [dbt](https://www.getdbt.com).
- **Mappers do not support joins between streams.**
  - To join data, first land the data and then perform joins using a
    transformation tool like [dbt](https://www.getdbt.com).
- **Mappers do not support external API lookups.**
  - To add external API lookups, you can either (a) land all your data and then joins using
    a transformation tool like [dbt](https://www.getdbt.com), or (b) create a custom mapper
    plugin with inline lookup logic.

## A feature for all Singer users, enabled by the SDK

The mapping features described here are created for the **_users_** of SDK-based taps and targets, which support inline transformations with `stream_maps` and `stream_map_config` out-of-box.

**Note:** to support non-SDK taps and targets, the standalone inline mapper plugin [`meltano-map-transformer`](https://hub.meltano.com/mappers/meltano-map-transformer/) follows all specifications defined here and can apply mapping transformations between _any_ Singer tap and target, even if they are not built using the SDK.

The following behaviors are implemented by the SDK automatically:

1. For taps, the SCHEMA and RECORD messages will automatically be transformed,
   duplicated, filtered, or aliased, as per the `stream_maps` config settings _after_
   all other tap-specific logic is executed.
   - Because this process happens automatically after all other tap logic is executed, the
     tap developer does not have to write any custom handling logic.
   - The tap development process is fully insulated from this 'out-of-box' functionality.
2. Similarly for targets, the received streams are processed by the `stream_maps` config
   setting _prior_ to any Sink processing functions.
   - This means that the target developer can assume that all streams and records are
     transformed, aliased, filtered, etc. _before_ any custom target code is executed.
3. The standalone mapper plugin [`meltano-map-transformer`](https://hub.meltano.com/mappers/meltano-map-transformer/) is a hybrid tap/target which
   simply receives input from a tap, transforms all stream and schema messages via the
   `stream_maps` config option, and then emits the resulting stream(s) to a downstream
   target.
   - A standalone mapper is not needed in cases where either the tap or target is built
     on the SDK (since either could accept the `stream_maps` config option) but it is useful
     in cases where using legacy taps or targets which do not yet support this
     functionality - or in cases where you want to run a one-time sync with special logic
     and otherwise keep tap and target config untouched.

## Constructing the `stream_maps` config object

The `stream_maps` config expects a mapping of stream names to a structured transform object.

Here is a sample `stream_maps` transformation which obfuscates `phone_number` with a fake value, removes all references to `email` and adds `email_domain` and `email_hash` as new properties:

`meltano.yml` or `config.json`:

````{tab} meltano.yml
```yaml
stream_maps:
  # Apply these transforms to the stream called 'customers'
  customers:
    # drop the PII field from RECORD and SCHEMA messages
    email: __NULL__
    # capture just the email domain
    email_domain: owner_email.split('@')[-1]
    # for uniqueness checks
    email_hash: md5(config['hash_seed'] + owner_email)
    # generate a fake phone number
    phone_number: fake.phone_number()
stream_map_config:
  # hash outputs are not able to be replicated without the original seed:
  hash_seed: 01AWZh7A6DzGm6iJZZ2T
faker_config:
  # set specific seed
  seed: 0
  # set specific locales
  locale:
  - en_US
  - en_GB
```
````

````{tab} JSON
```json
{
    "stream_maps": {
        "customers": {
            "email": null,
            "email_domain": "owner_email.split('@')[-1]",
            "email_hash": "md5(config['hash_seed'] + owner_email)",
            "phone_number": "fake.phone_number()"
        }
    },
    "stream_map_config": {
        "hash_seed": "01AWZh7A6DzGm6iJZZ2T"
    },
    "faker_config": {
        "seed": 0,
        "locale": [
            "en_US",
            "en_GB"
        ]
    }
}
```
````

If map expressions should have access to special config, such as in the
one-way hash algorithm above, define those config arguments within the optional
`stream_map_config` setting. Values defined in `stream_map_config` will be available
to expressions using the `config` dictionary.

### Constructing Expressions

Expressions are defined and parsed using the
[`simpleeval`](https://github.com/danthedeckie/simpleeval) expression library. This library
accepts most native python expressions and is extended by custom functions which have been declared
within the SDK.

#### Compound Expressions

Starting in version 0.33.0, the SDK supports the use of simple comprehensions, e.g. `[x + 1 for x in [1,2,3]]`. This is a powerful feature which allows you to perform complex transformations on lists of values. For example, you can use comprehensions to filter out values in an array:

````{tab} meltano.yml
```yaml
stream_maps:
  users:
    id: id
    fields: "[f for f in fields if f['key'] != 'age']"
```
````

````{tab} JSON
```json
{
  "stream_maps": {
    "users": {
      "id": "id",
      "fields": "[f for f in fields if f['key'] != 'age']"
    }
  }
}
```
````

### Accessing Stream Properties within Mapping Expressions

By default, all stream properties are made available via the property's given name. For
instance, assuming a field called `customer_id` in the stream, you can write
`customer_id.lower()` to apply Python's `lower()` function to all customer IDs.

Note: In some cases, property names may collide with built-in functions or keywords. To
handle these cases, use the `record` name as described below (or its shorthand `_`).

These are all equivalent means of transforming the `customer_id` property of the current
record:

- `customer_id.lower()`
- `record['customer_id'].lower()`
- `_['customer_id'].lower()`

### Other Built-in Functions and Names

Currently, there are a small handful of convenience functions and object aliases, which
can be referenced directly by mapping expressions.

#### Built-In Functions

The following functions and namespaces are available for use in mapping expressions:

| Function                                             | Description                                                                                                                                                                                                                                                                  |
| :--------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`md5()`](inv:python:py:function:#hashlib.md5)       | Returns an inline MD5 hash of any string, outputting the string representation of the hash's hex digest. This is defined by the SDK internally with native python: [`hashlib.md5(<input>.encode("utf-8")).hexdigest()`](inv:python:py:method:#hashlib.hash.hexdigest).       |
| [`sha256()`](inv:python:py:function:#hashlib.sha256) | Returns an inline SHA256 hash of any string, outputting the string representation of the hash's hex digest. This is defined by the SDK internally with native python: [`hashlib.sha256(<input>.encode("utf-8")).hexdigest()`](inv:python:py:method:#hashlib.hash.hexdigest). |
| [`datetime`](inv:python:py:module:#datetime)         | This is the datetime module object from the Python standard library. You can access [`datetime.datetime`](inv:python:py:class:#datetime.datetime), [`datetime.timedelta`](inv:python:py:class:#datetime.timedelta), etc.                                                     |
| [`json`](inv:python:py:module:#json)                 | This is the json module object from the Python standard library. Primarily used for calling [`json.dumps()`](inv:python:py:function:#json.dumps) and [`json.loads()`](inv:python:py:function:#json.loads).                                                                   |

#### Built-in Variable Names

The following variables are available in the context of a mapping expression:

| Variable          | Description                                                                                                                                                                                       |
| :---------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `config`          | A dictionary with the `stream_map_config` values from settings. This can be used to provide a secret hash seed, for instance.                                                                     |
| `record`          | An alias for the record values dictionary in the current stream.                                                                                                                                  |
| `_`               | Same as `record` but shorter to type.                                                                                                                                                             |
| `self`            | The existing property value if the property already exists.                                                                                                                                       |
| `fake`            | A [`Faker`](inv:faker:std:doc#index) instance, configurable via `faker_config` (see previous example) - see the built-in [standard providers](inv:faker:std:doc#providers) for available methods. |
| `__stream_name__` | The name of the stream. Useful when [applying the same transformation to multiple streams](#applying-a-mapping-across-two-or-more-streams).                                                       |

  ```{tip}
  To use the `fake` object, the `faker` library must be installed.
  ```

:::{versionadded} 0.35.0
The `faker` object.
:::

:::{versionadded} 0.40.0
The `Faker` class.
:::

:::{versionchanged} 0.41.0
The `Faker` class was deprecated in favor of instance methods on the `fake` object.
:::

:::{versionadded} 0.42.0
The `__stream_name__` variable.
:::

#### Built-in Alias Variable Names

The following variables are available in the context of the `__alias__` expression:

| Variable          | Description              |
| :---------------- | :----------------------- |
| `__stream_name__` | The existing stream name |

:::{versionadded} 0.42.0
The `__stream_name__` variable.
:::

#### Automatic Schema Detection

For performance reasons, type detection is performed at runtime using text analysis
of the provided expressions. Type detection is performed once per stream, prior to
records being generated.

The following logic is applied in determining the SCHEMA of the transformed stream:

1. Calculations which begin with the text `str(`, `float(`, `int(` will be
   assumed to be belonging to the specified type.
2. Otherwise, if the property already existed in the original stream, it will be assumed
   to have the same data type as the original stream.
3. Otherwise, if no type is detected using the above rules, any new stream properties will
   be assumed to be of type `str` .

## Customized `stream_map` Behaviors

### Removing a single stream or property

To remove a stream, declare the stream within `stream_maps` config and assign it the value
`null`. For example:

````{tab} meltano.yml
```yaml
stream_maps:
  # don't sync the stream called 'addresses'
  addresses: __NULL__
```
````

````{tab} JSON
```json
{
    "stream_maps": {
        "addresses": null
    }
}
```
````

To remove a property, declare the property within the designated stream's map entry and
assign it the value `null`. For example:

````{tab} meltano.yml
```yaml
stream_maps:
  customers:
    # don't sync the 'email' stream property
    email: __NULL__
```
````

````{tab} JSON
```json
{
    "stream_maps": {
        "customers": {
            "email": null
        }
    }
}
```
````

### Remove all undeclared streams or properties

By default, all streams and stream properties will be included in the output unless
specifically excluded. However, you can reverse this behavior using the `"__else__": null`
instruction to only include defined properties or streams.

**Note:** the primary key properties of the stream will still be included by default, to ensure proper deduping and
record identification at the target. To also remove primary keys from the stream, see the `__key_properties__` override
below.

To remove all streams except the `customers` stream:

````{tab} meltano.yml
```yaml
stream_maps:
  customers: {}
  __else__: __NULL__
```
````

````{tab} JSON
```json
{
    "stream_maps": {
        "customers": {},
        "__else__": null
    }
}
```
````

To remove all fields from the `customers` stream except `customer_id`:

````{tab} meltano.yml
```yaml
stream_maps:
  customers:
    customer_id: customer_id
    __else__: __NULL__
```
````

````{tab} JSON
```json
{
    "stream_maps": {
        "customers": {
            "customer_id": "customer_id",
            "__else__": null
        }
    }
}
```
````

### Unset or modify the stream's primary key behavior

To override the stream's default primary key properties, add the `__key_properties__` operation within the stream map definition.

````{tab} meltano.yml
```yaml
stream_maps:
  customers:
    # Remove the original Customer ID column
    customer_id: __NULL__
    # Add a new (and still unique) ID column
    customer_id_hashed: md5(customer_id)
    # Updated key to reflect the new name
    __key_properties__:
    - customer_id_hashed
```
````

````{tab} JSON
```json
{
    "stream_maps": {
        "customers": {
            "customer_id": null,
            "customer_id_hashed": "md5(customer_id)",
            "__key_properties__": ["customer_id_hashed"]
        }
    }
}
```
````

Notes:

- To sync the stream as if it did not contain a primary key, simply set `__key_properties__` to `null` or an empty list.
- Key properties _must_ be present in the transformed stream result. Otherwise, an error will be raised.

### Add a property with a string literal value

Some applications, such as multi-tenant, may benefit from adding a property with a hardcoded string literal value.
These values need to be wrapped in double quotes to differentiate them from property names:

````{tab} meltano.yml
```yaml
stream_maps:
  customers:
    a_new_field: '\"client-123\"'
```
````

````{tab} JSON
```json
{
    "stream_maps": {
        "customers": {
            "a_new_field": "\"client-123\""
        }
    }
}
```
````

### Masking data with Faker

It is best practice (or even a legal requirement) to mask PII/PHI in lower environments. Stream mappers have access to the `Faker` library, which can be used to generate random data in various forms/formats.

```yaml
stream_maps:
  customers:
    # IMPORTANT: the `fake` variable name will only be available if faker_config is defined
    first_name: fake.first_name() # generates a new random name each time
faker_config:
  # set specific seed
  seed: 0
  # set specific locales
  locale:
    - en_US
    - en_GB
```

Be sure to checkout the [`faker` documentation](https://faker.readthedocs.io/en/master/) for all the fake data generation possibilities.

Note that in the example above, `faker` will generate a new random value each time the `first_name()` function is invoked. This means if 3 records have a `first_name` value of `Mike`, then they will each have a different name after being mapped (for example, `Alistair`, `Debra`, `Scooby`).  This can actually lead to issues when developing in the lower environments.

Some users require consistent masking (for example, the first name `Mike` is always masked as `Debra`). Consistent masking preserves the relationship between tables and rows, while still hiding the real value. When a random mask is generated every time, relationships between tables/rows are effectively lost, making it impossible to test things like sql `JOIN`s. This can cause highly unpredictable behavior when running the same code in lower environments vs production.

To generate consistent masked values, you must provide the **same seed each time** before invoking the faker function.

```yaml
stream_maps:
  customers:
    # will always generate the same value for the same seed
    first_name: fake.seed_instance(_['first_name']) or fake.first_name()
faker_config:
  # IMPORTANT: `fake` is only available if the `faker` extra is installed
  locale: en_US
```

Remember, these expressions are evaluated by the [`simpleeval`](https://github.com/danthedeckie/simpleeval) expression library, which only allows a single python expression (which is the reason for the `or` syntax above).

This means if you require more advanced masking logic, which cannot be defined in a single python expression, you may need to consider a custom stream mapper.

### Aliasing a stream using `__alias__`

To alias a stream, simply add the operation `"__alias__": "new_name"` to the stream
definition. For example, to alias the `customers` stream as `customer_v2`, use the
following:

````{tab} meltano.yml
```yaml
stream_maps:
  customers:
    __alias__: customers_v2
```
````

````{tab} JSON
```json
{
    "stream_maps": {
        "customers": {
            "__alias__": "customers_v2"
        }
    }
}
```
````

### Duplicating or splitting a stream using `__source__`

To create a new stream as a copy of the original, specify the operation
`"__source__": "stream_name"`. For example, you can create a copy of the `customers` stream
which only contains PII properties using the following:

````{tab} meltano.yml
```yaml
stream_maps:
  customers:
    # Exclude these since we're capturing them in the pii stream
    email: __NULL__
    full_name: __NULL__
  customers_pii:
    __source__: customers
    # include just the PII and the customer_id
    customer_id: customer_id
    email: email
    full_name: full_name
    # exclude anything not declared
    __else__: __NULL__
```
````

````{tab} JSON
```json
{
    "stream_maps": {
        "customers": {
            "email": null,
            "full_name": null
        },
        "customers_pii": {
            "__source__": "customers",
            "customer_id": "customer_id",
            "email": "email",
            "full_name": "full_name",
            "__else__": null
        }
    }
}
```
````

### Filtering out records from a stream using `__filter__` operation

The `__filter__` operation accepts a string expression which must evaluate to `true` or
`false`. Filter expressions should be wrapped in `bool()` to ensure proper type conversion.

For example, to only include customers with emails from the `example.com` company domain:

````{tab} meltano.yml
```yaml
stream_maps:
  customers:
    __filter__: email.endswith('@example.com')
```
````

````{tab} JSON
```json
{
    "stream_maps": {
        "customers": {
            "__filter__": "email.endswith('@example.com')"
        }
    }
}
```
````

### Aliasing properties

This uses a "copy-and-delete" approach with the help of `__NULL__`:

````{tab} meltano.yml
```yaml
stream_maps:
  customers:
    new_field: old_field
    old_field: __NULL__
```
````

````{tab} JSON
```json
{
    "stream_maps": {
        "customers": {
            "new_field": "old_field",
            "old_field": "__NULL__"
        }
    }
}
```
````

### Applying a mapping across two or more streams

You can use glob expressions to apply a stream map configuration to more than one stream:

````{tab} meltano.yml
```yaml
stream_maps:
  "*":
    name: first_name
    first_name: __NULL__
```
````

````{tab} JSON
```json
{
    "stream_maps": {
        "*": {
            "name": "first_name",
            "first_name": "__NULL__"
        }
    }
}
```
````

:::{versionadded} 0.37.0
Support for stream glob expressions.
:::

### Aliasing two or more streams

The `__alias__` operation evaluates simple python expressions.

You can combine this with glob expressions to rename more than one stream:

````{tab} meltano.yml
```yaml
stream_maps:
  "*":
    __alias__: "__stream_name__ + '_v2'"
```
````

````{tab} JSON
```json
{
    "stream_maps": {
        "*": {
            "__alias__": "__stream_name__ + '_v2'"
        }
    }
}
```
````

:::{versionadded} 0.42.0
Support for `__alias__` expression evaluation.
:::

### Understanding Filters' Affects on Parent-Child Streams

Nested child streams iterations will be skipped if their parent stream has a record-level
filter applied. This applies only to the primary map for a stream, ignoring any filter
logic on duplicate streams created using `__source__`.

If you want to prevent child streams from being filtered, create a duplicate stream using
`__source__` and filter the duplicated stream instead of the original. Then, if you do not
want to emit two streams, suppress the original by assigning its map the value `null`.
Since the primary (same-named) version of the stream is not filtered, this will force
iteration through the entire source stream and its children, while only
emitting those parent records that qualify the filter expression.

**Note:** aliasing a stream with the `__alias__` operation does not impact child stream selection logic.

## Known Limitations

_The below functionality may be expanded or improved in the future. Please send us an
Issue or MR if you are interested in contributing to these features._

### No nested property declarations or removals

Only first-level properties may be added, removed, or transformed. This means, for example, that you can
add or remove a top-level field called `customer_email`, but you cannot add or remove a
nested `email` property if embedded in a `customer` json object.

### Schema detection capabilities are limited

Schema detection currently relies on somewhat naive static text parsing. The workaround for
the user is fairly trivial - which is to send hints by wrapping the entire expression in
`str()`, `float()`, `int()`, etc. While this is perhaps not optimal, it meets our core
requirement for static type evaluation with minimal config complexity.

### Security Implications for Low-Trust Environments

While `simpleeval` does provide some isolation and sandboxing capabilities built-in, there
are always security implications when allowing user-provided code to run on managed servers.
For this reason, administrators should not permit arbitrary setting injection
from untrusted users. As a rule, tap and target settings should never be permitted to be modified
by untrusted users.

### Else behavior currently limited to `null` assignment

The only operation currently allowed for the `__else__` instruction is `null`, meaning
to exclude any streams or properties not otherwise defined. In the future, we may add
additional options or advanced logic. For instance, we could in the future add the ability
to remove or treat a property from any stream in which it appears. We could also hash
any properties not otherwise declared in the map (for PII reasons and to enable advanced
testing scenarios).

## Q&A

### Q: How do stream map operations interact with stream selection via the Singer catalog metadata?

**Answer:** Stream maps are applied only _after_ stream selection rules are applied. This means that if a stream or property is not selected, it will not be available for stream map operations. Stream maps are not intended to be a replacement for catalog-based selection, but they may be used to further refine streams beyond the original selection parameters.

### Q: If streams are excluded by applying mapping rules, does the tap automatically skip them?

**Answer:** It depends. For SDK-based taps, yes. If an entire stream is specified to be
excluded at the tap level, then the stream will be skipped exactly as if it were deselected
in the catalog metadata.

If a stream is specified to be excluded at the target level, or in a standalone mapper
between the tap and target, the filtering occurs downstream from the tap and therefore cannot
affect the selection rules of the tap itself. Except in special test cases or in cases where
runtime is trivial, we highly recommend implementing stream-level exclusions at the tap
level rather than within the downstream target or mapper plugins.

### Q: Why use a separate `stream_map_config` option instead of granting access to all `config` values?

**Answer:** The base-level config is also the primary mechanism for submitting auth secrets
to the plugin. If we provided direct access to all config options, it would drastically
increase the security risks associated with code injection and accidental or malicious
leakage of credentials to downstream logs. By limiting to only those config values intended
for use by the mapper, we significantly improve the security profile of the feature.
Additionally, plugins are generally expected to fail if they receive unexpected config
arguments. The intended use cases for stream map config values are user-defined in nature
(such as the hashing use case defined above), and are unlikely to overlap with the
plugin's already-existing settings.

### Q: What is the difference between `primary_keys` and `key_properties`?

**Answer:** These two are _generally_ identical - and will only differ in cases like the above where `key_properties` is manually
overridden or nullified by the user of the tap. Developers will specify `primary_keys` for each stream in the tap,
but they do not control if the user will override `key_properties` behavior when initializing the stream. Primary keys
describe the nature of the upstream data as known by the source system. However, either through manual catalog manipulation and/or by
setting stream map transformations, the in-flight dedupe keys (`key_properties`) may be overridden or nullified by the user at any time.

Additionally, some targets do not support primary key distinctions, and there are valid use cases to intentionally unset
the `key_properties` in an extract-load pipeline. For instance, it is common to intentionally nullify key properties to trigger
"append-only" loading behavior in certain targets, as may be required for historical reporting. This does not change the
underlying nature of the `primary_key` configuration in the upstream source data, only how it will be landed or deduped
in the downstream source.


### Q: How do I use Meltano environment variables to configure stream maps?

**Answer:** Environment variables in Meltano can be used to configure stream maps, but you first need to add the corresponding settings
to your plugins `settings` option. For example:

```yaml
plugins:
  extractors:
  - name: tap-csv
    variant: meltanolabs
    pip_url: git+https://github.com/MeltanoLabs/tap-csv.git
    settings:
    - name: stream_maps.customers.email
    - name: stream_maps.customers.email_domain
    - name: stream_maps.customers.email_hash
    - name: stream_maps.customers.__else__
    - name: stream_maps.stream_map_config
```

Then, you can set the following environment variables:

```shell
TAP_CSV_STREAM_MAPS_CUSTOMERS_EMAIL_DOMAIN='email.split("@")[-1]'
TAP_CSV_STREAM_MAPS_CUSTOMERS_EMAIL_HASH='md5(config["hash_seed"] + email)'
TAP_CSV_STREAM_MAP_CONFIG_HASH_SEED='01AWZh7A6DzGm6iJZZ2T'
```
