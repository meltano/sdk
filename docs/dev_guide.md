# Tap-Base Development Docs

**Development Overview:**

_Developing with `tap-base` requires overriding three classes:_

- `TapBase` - _The core base class for taps. This class governs naming, configuration, and core capability mapping._
- `GenericConnectionBase` - _The base class for generic-type connections. This class is responsible for making a connection to the source, sending queries, and retrieving metadata._
- `DatabaseConnectionBase` - _The base class for database-type connections. Adds specialized functionality for database-type connections._
- `TapStreamBase` - _The base class for streams. This class is responsible for replication and bookmarking._

**Detailed Instructions:**

1. [Steps for developing a new tap:](#steps-for-developing-a-new-tap)
   - [Step 1: Create a new project from the `tap-template` CookieCutter repo](#step-1-create-a-new-project-from-the-tap-template-cookiecutter-repo)
   - [Step 2: Write and test the Tap class](#step-2-write-and-test-the-tap-class)
   - [Step 3: Write and test the Connection class](#step-3-write-and-test-the-connection-class)
   - [Step 3: Write and test the Stream class](#step-3-write-and-test-the-stream-class)
   - [Step 4: Add more tests](#step-4-add-more-tests)
2. [Troubleshooting Tips](#troubleshooting-tips)

## Steps for developing a new tap:

### Step 1: Create a new project from the `tap-template` [CookieCutter](https://cookiecutter.readthedocs.io) repo

`TODO: TK - write cookiecutter instructions`

### Step 2: Write and test the Tap class

`TODO: TK - write tap config instructions`

### Step 3: Write and test the Connection class

`TODO: TK - write connection config instructions`

### Step 3: Write and test the Stream class

`TODO: TK - write stream config instructions`

### Step 4: Add more tests

`TODO: TK - write test writing instructions`

## Troubleshooting Tips

`TODO: TK - write troubleshooting tips`
