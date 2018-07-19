# rate-limiter-py

This is a rate-limiter module which leverages DynamoDB to enforce resource limits.

This module offers two different methods for consuming and replenishing resource capacity.
The typical use calls for a time driven method for distributing and replenishing tokens, e.g. 10 requests per second.
However, there are cases which require both an acquisition and return of a specific token,
e.g. wait for an EMR cluster to complete.

Bearing this in mind, resource capacity is represented by either fungible
or non-fungible tokens. The former are interchangeable and thus can
make use of time-based expiration to restore capacity.
The latter are unique and require explicit removal to restore capacity.

For fungible tokens, the limiter leverages the token bucket algorithm,
tracking resource usage by acquiring and replenishing tokens for each unit of capacity.

For non-fungible tokens, the limiter creates a new token for each resource.
Limiting is enforced by disallowing token creation beyond the specified capacity.

## Fungible Token Requirements and Usage

Fungible token rate-limiting requirements and usage is detailed below.

### DynamoDB Tables

The expected usage is each rate limiter will use the same multi-tenant token and limit tables, created and
managed by a separate service. However, a private token and/or limit table can be used when instantiating the
middleware.

#### Token Table

The tokens for a single resource are stored in a single DynamoDB row, representing the "bucket".
The expected table schema is detailed below.

##### Attributes

These are all the expected table attributes, including the keys.

| Attribute Name | Data Type | Description                                                  |
|----------------|-----------|--------------------------------------------------------------|
| resourceName   | String    | User-defined name of the rate limited resource               |
| accountId      | String    | Id of the entity which created the resource                  |
| tokens         | Number    | Number of tokens available                                   |
| lastRefill     | Number    | Timestamp, in milliseconds, when the tokens were replenished |
| lastToken      | Number    | Timestamp, in milliseconds, when the last token was taken    |

##### Keys

The key data type and description can be found in the above, attributes table.

| Attribute Name | Key Type |
|----------------|----------|
| resourceName   | HASH     |
| accountId      | RANGE    |

#### Limit Table

The limit and window for a specific account on a specific resource are stored in a single DynamoDB row.
The expected table schema is detailed below.

##### Attributes

These are all the expected table attributes, including the keys.

| Attribute Name | Data Type | Description                                                                                    |
|----------------|-----------|------------------------------------------------------------------------------------------------|
| resourceName   | String    | User-defined name of the rate limited resource                                                 |
| accountId      | String    | Id of the entity which created the resource                                                    |
| limit          | Number    | The maximum number of tokens the account may acquire on the resource                           |
| windowSec      | Number    | Sliding window of time, in seconds, wherein only the limit number of tokens will be available. |
| serviceName    | String    | Name of the service that created this limit.                                                   |

##### Keys

The key data type and description can be found in the above, attributes table.

| Attribute Name | Key Type |
|----------------|----------|
| resourceName   | HASH     |
| accountId      | RANGE    |

##### Service Limits Index

The service limits global secondary index is used when updating/loading service limits.

| Attribute Name | Key Type |
|----------------|----------|
| serviceName    | HASH     |

### Usage

Each of the fungible token limiter implementations require the names of the token and limit tables. These values can be
passed directly to the limiter or set via environment variables.

| Name        | Environment Variable | Description                                          |
|-------------|----------------------|------------------------------------------------------|
| token_table | FUNGIBLE_TABLE       | Name of the DynamoDB table containing tokens.        |
| limit_table | LIMIT_TABLE          | Name of the DynamoDB table containing account limit. |

The only other value required by each implementation is `account id`. Each implementation handles specifying this value
differently.

#### Decorator

The function decorator has a minimum of four arguments:

1.  The name of the resource being rate-limited.

1.  How to access the account id from the arguments of the function being decorated. This can be either a positional
    argument numeric index or a keyword argument key.

1.  The default limit, used when no limit is found in the limits table.

1.  The default window, used when no window is found in the limits table.

##### Examples

The examples below assume the table name, limit and window have been set via environment variables.

###### Positional Argument

```python
from limiter import rate_limit

@rate_limit('my-resource', 1, 10, account_id_pos=1)
def invoke_my_resource(arg_1, account_id):
  # If I am here, I was not rate limited
```

###### Keyword Argument

```python
from limiter import rate_limit

@rate_limit('my-resource', 1, 10, account_id_key='foo')
def invoke_my_resource(arg_1, foo='account-1234'):
  # If I am here, I was not rate limited

# The default keyword argument is "account_id", to make the decorator more succinct:
@rate_limit('my-resource', 1, 10)
def invoke_my_resource(arg_1, account_id='account-1234'):
  # If I am here, I was not rate limited
```

#### Context Manager

The context manager has a minimum of four arguments:

1.  The name of the resource being rate-limited.

1.  The account id.

1.  The default limit, used when no limit is found in the limits table.

1.  The default window, used when no window is found in the limits table.

##### Example

The example below assume the table name, limit and window have been set via environment variables.

```python
from limiter import fungible_limiter

def invoke_my_resource(account_id):
  with fungible_limiter('my-resource', account_id, 1, 10):
    # If I am here, I was not rate limited
```

#### Direct

Directly creating an instance of the fungible limiter has a minimum of four arguments:

1.  The name of the resource being rate-limited.

1.  The account id.

1.  The default limit, used when no limit is found in the limits table.

1.  The default window, used when no window is found in the limits table.

##### Example

The example below assume the table name, limit and window have been set via environment variables.

```python
from limiter import fungible_limiter

def invoke_my_resource(account_id):
  limiter = fungible_limiter('my-resource', account_id, 1, 10)
  limiter.get_token()
  # If I am here, I was not rate limited
```

## Non-Fungible Token Requirements and Usage

Non-fungible token rate-limiting requirements and usage is detailed below.

### DynamoDB Table

Each token is represented as a single row in DynamoDB. The expected table schema is detailed below.

#### Attributes

These are all the expected table attributes, including the keys.

| Attribute Name     | Data Type | Description                                                   |
|--------------------|-----------|---------------------------------------------------------------|
| resourceCoordinate | String    | Composed of the resource name and account id                  |
| reservationId      | String    | Identifies the token reservation                              |
| resourceId         | String    | Identifies the instance of a resource, e.g. EMR cluster id    |
| resourceName       | String    | User-defined name of the rate limited resource                |
| expirationTime     | Number    | Timestamp, in sec, when the token will be expired by DynamoDB |
| accountId          | String    | Id of the entity which created the resource                   |


#### Keys

The key data type and description can be found in the above, attributes table.

| Attribute Name     | Key Type |
|--------------------|----------|
| resourceCoordinate | HASH     |
| reservationId      | RANGE    |

#### Global Secondary Index

A global secondary index is used to locate tokens using only `resourceId`. This will be needed to locate tokens
based on the resource id provided in CloudWatch events.

| Attribute Name | Key Type |
|----------------|----------|
| resourceId     | HASH     |

### Creating Tokens

Each of the fungible token limiter implementations require the names of the token and limit tables. These values can be
passed directly to the limiter or set via environment variables.

| Name        | Environment Variable | Description                                         |
|-------------|----------------------|-----------------------------------------------------|
| token_table | FUNGIBLE_TABLE       | Name of the DynamoDB table containing tokens.       |
| limit_table | LIMIT_TABLE          | Name of the DynamoDB table containing account limit |

The only other value required by each implementation is `account id`. Each implementation handles specifying this value
differently.

Each implementation example assumes the table name and limit have been set via environment variables.

#### Context Manager

The context manager has a minimum of three arguments:

1.  The name of the resource being rate-limited.

1.  The account id.

1.  The default limit, used when no limit is found in the limits table.

##### Example

```python
from limiter import non_fungible_limiter

def invoke_my_resource(account_id):
  with non_fungible_limiter('my-resource', account_id, 10) as reservation:
    emr_cluster_id = create_emr_cluster() # Create an instance of the resource
    reservation.create_token(emr_cluster_id) # Create a token for this unique resource
```

#### Directly

Directly creating an instance of the non-fungible limiter has a minimum of three arguments:

1.  The name of the resource being rate-limited.

1.  The account id.

1.  The default limit, used when no limit is found in the limits table.

##### Example

```python
from limiter import non_fungible_limiter

def invoke_my_resource(account_id):
  limiter = non_fungible_limiter('my-resource', account_id, 10)
  reservation = limiter.get_reservation()

  emr_cluster_id = create_emr_cluster() # Create an instance of the resource
  reservation.create_token(emr_cluster_id) # Create a token for this unique resource
```

### Removing Tokens

The recommended approach for detecting the termination of and removing tokenized
resources is via a Lambda triggered by CloudWatch. The CloudWatch rules should be as precise as
practical to avoid unnecessarily executing the lambda.

The `event_processors` module contains all the logic necessary to consume, test and remove tokens from
CloudWatch events.

#### Usage

The `EventProcessorManager` class removes non-fungible tokens from DynamoDB represented by CloudWatch events.
The manager is composed of multiple `EventProcessors`, one for specific event "source", e.g. 'aws.emr'.
The processors are responsible for determining if an event references a tokenized resource and if so, extracting its
resource id. Each processor is composed of zero to many predicates, which determine if an event references a tokenized
resource. If a processor is not configured with any predicates it will just extract the resource id.

##### Example

```python
from limiter.event_processors import EventProcessorManager, EventProcessor, ProcessorPredicate

predicate = ProcessorPredicate('detail.name', lambda name: 'debugging' not in name)
processor = EventProcessor('aws.emr', 'detail.clusterId', predicate=predicate)
manager = EventProcessorManager(table_name='table', index_name='idx', processors=[processor])

def handler(event, context):
  manager.process_event(event)
```

## Development

### Dependencies
The dependencies needed for local development (running unit tests, etc.) are contained in `dev_requirements.txt` and
can be installed via pip: `pip install -r dev_requirements.txt`.

### Unit Tests
Running the unit tests is done via a recipe in the `makefile`, the command: `make test`.
The unit tests are run with [nose](http://nose.readthedocs.io/en/latest/) inside a virtual environment managed
by [tox](https://pypi.python.org/pypi/tox). The `test_requirements.txt` contains all the testing dependencies and
is used to pip install everything needed by the tests in the tox environments (tox installs these dependencies).

### Code Hygiene
The `make check` command will run [pylint](https://www.pylint.org/) with standards defined in `pylintrc`.
This is a measurable way to enforce style and standards.

### Cleanup
Run `make clean` to remove artifacts leftover by tox and pylint.
