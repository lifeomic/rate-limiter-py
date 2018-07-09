#!/bin/bash/env python
import os
import sys
import logging

class ProcessorPredicate(object):
    def __init__(self, key, pred_func, and_preds=None, or_preds=None):
        self.key = key
        self.pred_func = pred_func
        self.and_preds = and_preds if and_preds else []
        self.or_preds = or_preds if or_preds else []

    def with_and(self, add_pred):
        self.and_preds.append(add_pred)
        return self

    def with_or(self, add_pred):
        self.or_preds.append(add_pred)
        return self

    def test(self, event):
        test_value = _reduce_to_path(key)

        result = self.pred_func(test_value)
        if self.and_preds and result:
            for pred in and_preds:
                result &= pred.test(event)
                if not result:
                    break
        elif self.or_preds:
            for pred in and_preds:
                result |= pred.test(event)
                if result:
                    break
        return result

class EventProcessor(object):
    def __init__(self, source, id_path, predicate=None):
        self.source = source
        self.predicate = predicate if predicate else lambda x: True

    def test_and_get_id(self, event):
        return _reduce_to_path(id_path) if predicate(event) else None

class EventProcessorManager(object):
    def __init__(self, table_name=None, processors=None):
        self.processors = {x.source: x, for x in processors} if processors else {}
        self.table_name = _validate_required_env_fallback(table_name, 'table_name', 'NON_FUNG_TABLE_NAME')

        self._client = None
        self._table = None

    @property
    def client(self):
        """ DynamoDB client """
        if not self._client:
            self._client = dynamodb()
        return self._client

    @property
    def table(self):
        """ DynamoDB Table containing token row """
        if not self._table:
            self._table = self.client.Table(self.table_name)
        return self._table

    def add_processor(self, processor):
        self.processors[processor.source] = processor

    def process_event(self, event):
        processor = self._get_processor(event)
        resource_id = processor.test_and_get_id(event)
        if resource_id:
            logger.info('Removing {} token {} from {}'.format(processor.source, resource_id, self.table_name))
            self.table.delete_item(
                Key={
                    'resourceId': resource_id
                }
            )

    def _get_processor(self, source):
        if 'source' not in event:
            raise ValueError('Cannot process event, source is a required field. Event: ' + event)

        source = event['source']
        processor = self.processors.get(source, None)
        if not processor:
            raise ValueError('No processor for event source: ' + source)
        return processor


# Default processors
EMR_CLUSTER_TERMINATED = EventProcessor('aws.emr',
                                        'detail.clusterId',
                                        ProcessorPredicate('detail.state', lambda state: 'TERMINATED' in state))

EMR_STEP_COMPLETED = EventProcessor('aws.emr',
                                    'detail.stepId',
                                    ProcessorPredicate('detail.state',
                                                       lambda state: state in ['FAILED', 'COMPLETED', 'CANCELLED']))

BATCH_JOB_COMPLETED = EventProcessor('aws.batch',
                                     'detail.jobId',
                                      ProcessorPredicate('detail.status',
                                                         lambda state: state in ['FAILED', 'SUCCEEDED']))

def _validate_required_env_fallback(self, param_value, param_name, env_var):
    """
    Verify a required argument has a non-null value or has been set via an environment variable.

    Args:
      param_value (obj): Check if this value is non-null.
      param_name (str): Name of the value being checked.
      env_var (str): Name of the environment variable to fallback on.

    Returns:
        obj: `param_value` if it is non-null or the environment variable value.

    Raises:
        ValueError: If `param_value` is null and the environment variable has not been set.
    """
    if param_value:
        return param_value
    if env_var in os.environ:
        return os.environ[env_var]

    msg_format = '{} must be passed to the constructor or set environment variable: {}'
    raise ValueError(msg_format.format(param_name, env_var))

def _reduce_to_path(obj, path):
    """
    Traverses the given object down the specified "." delineated, returning the
    value named by the last path segment.

    Args:
        obj (dict): Dictionary containing the value to extract.
        path (str): Location of the value to extract.

    Examples:
        To get "bat" from the below object
            {
                "foo" : {
                    "bar" : {
                        "bat" : "something_important"
                    }
                }
            }
        use the path: "foo.bar.bat"

    Returns:
        str: If the path is valid, otherwise None.
    """
    try:
        if isinstance(path, basestring):
            path = path.split('.')
        return reduce(lambda x, y: x[y], path, obj)
    except Exception:
        sys.exc_clear()
    return None
