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

For fungible tokens, the limiter leverages the leaky token bucket algorithm,
tracking resource usage by acquiring and replenishing tokens for each unit of capacity.

For non-fungible tokens, the limiter creates a new token for each resource.
Limiting is enforced by disallowing token creation beyond the specified capacity.

## Fungible Token Requirements and Usage

Fungible token rate-limiting requirements and usage is detailed below.

### DynamoDB Table

The tokens for a single resource are stored in a single DynamoDB row, representing the "bucket".
The expected table schema is detailed below.

#### Attributes

These are all the expected table attributes, including the keys.

| Attribute Name | Data Type | Description                                         |
|----------------|-----------|-----------------------------------------------------|
| resourceName   | String    | User-defined name of the rate limited resource      |
| accountId      | String    | Id of the entity which created the resource         |
| tokens         | Number    | Number of tokens available                          |
| lastRefill     | Number    | Timestamp, in sec, when the tokens were replenished |


#### Keys

The key data type and description can be found in the above, attributes table.

| Attribute Name | Key Type |
|----------------|----------|
| resourceName   | HASH     |
| accountId      | RANGE    |

### Usage

Each of the fungible token limiter implementations require the following information. These values can be
passed directly to the limiter or set via environment variables.

| Name       | Environment Variable | Description                                                                                      |
|------------|----------------------|--------------------------------------------------------------------------------------------------|
| table_name | FUNG_TABLE_NAME      | Name of the DynamoDB table.                                                                      |
| limit      | FUNG_LIMIT           | The maximum number of tokens that may be available.                                              |
| window     | FUNG_WINDOW          | Sliding window of time, in seconds, wherein only the `limit` number of tokens will be available. |

The only other value required by each implementation is `account id`. Each implementation handles specifying this value
differently.

#### Decorator

The function decorator has a minimum of two arguments:

1.  The name of the resource being rate-limited.

1.  How to access the account id from the arguments of the function being decorated. This can be either a positional
    argument numeric index or a keyword argument key.

##### Examples

The examples below assume the table name, limit and window have been set via environment variables.

###### Positional Argument

```python
from limiter import rate_limit

@rate_limit('my-resource', account_id_pos=1)
def invoke_my_resource(arg_1, account_id):
  # If I am here, I was not rate limited
```

###### Keyword Argument

```python
from limiter import rate_limit

@rate_limit('my-resource', account_id_key='foo')
def invoke_my_resource(arg_1, foo='account-1234'):
  # If I am here, I was not rate limited

# The default keyword argument is "account_id", to make the decorator more succinct:
@rate_limit('my-resource')
def invoke_my_resource(arg_1, account_id='account-1234'):
  # If I am here, I was not rate limited
```

#### Context Manager

The context manager has a minimum of two arguments:

1.  The name of the resource being rate-limited.

1.  The account id.

##### Example

The example below assume the table name, limit and window have been set via environment variables.

```python
from limiter import fungible_limiter

def invoke_my_resource(account_id):
  with fungible_limiter('my-resource', account_id):
    # If I am here, I was not rate limited
```

#### Direct

Directly creating an instance of the fungible limiter has a minimum of two arguments:

1.  The name of the resource being rate-limited.

1.  The account id.

##### Example

The example below assume the table name, limit and window have been set via environment variables.

```python
from limiter import fungible_limiter

def invoke_my_resource(account_id):
  limiter = fungible_limiter('my-resource', account_id)
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
| resourceId         | String    | Identifies the instance of a resource, e.g. EMR cluster id    |
| resourceName       | String    | User-defined name of the rate limited resource                |
| expirationTime     | Number    | Timestamp, in sec, when the token will be expired by DynamoDB |
| accountId          | String    | Id of the entity which created the resource                   |


#### Keys

The key data type and description can be found in the above, attributes table.

| Attribute Name     | Key Type |
|--------------------|----------|
| resourceCoordinate | HASH     |
| resourceId         | RANGE    |

### Creating Tokens

Each of the non-fungible token limiter implementations require the following information. These values can be
passed directly to the limiter or set via environment variables.

| Name       | Environment Variable | Description                                                   |
|------------|----------------------|---------------------------------------------------------------|
| table_name | NON_FUNG_TABLE_NAME  | Name of the DynamoDB table                                    |
| limit      | NON_FUNG_LIMIT       | The maximum number of tokens/reservations that may be present |

Each implementation example assumes the table name and limit have been set via environment variables.

#### Context Manager

The context manager has a minimum of two arguments:

1.  The name of the resource being rate-limited.

1.  The account id.

##### Example

```python
from limiter import non_fungible_limiter

def invoke_my_resource(account_id):
  with non_fungible_limiter('my-resource', account_id) as reservation:
    emr_cluster_id = create_emr_cluster() # Create an instance of the resource
    reservation.create_token(emr_cluster_id) # Create a token for this unique resource
```

#### Directly

Directly creating an instance of the non-fungible limiter has a minimum of two arguments:

1.  The name of the resource being rate-limited.

1.  The account id.

##### Example

```python
from limiter import non_fungible_limiter

def invoke_my_resource(account_id):
  limiter = non_fungible_limiter('my-resource', account_id)
  reservation = limiter.get_reservation()

  emr_cluster_id = create_emr_cluster() # Create an instance of the resource
  reservation.create_token(emr_cluster_id) # Create a token for this unique resource
```

### Removing Tokens

The recommended architecture for detecting the termination of and removing tokenized
resources is via a Lambda consuming CloudWatch events. Details and examples coming
in the next set of changes.
