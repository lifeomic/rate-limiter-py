#!/bin/bash/env python
import os
import sys
import logging
from boto3.dynamodb.conditions import Key
from limiter.clients import dynamodb
from limiter.managers import RESOURCE_COORDINATE, RESOURCE_ID

logger = logging.getLogger()

class ProcessorPredicate(object):
    """
    Determines if an event matches a specific criteria.

    Instances of this class are used to determine if an event value, at the specified
    path, evaluates to True by the given function. Instances my also be composed of additional
    predicates. The additional predicates are either conjunctive (and) or disjunctive (or).

    Args:
        key (str): Location of the value to evalute with the given function.
        pred_func (callable): Function which accepts a single argument, the value, and returns True or False
        and_preds (list:ProcessorPredicate): List of conjunctive ProcessorPredicates. Defaults to an empty list.
        or_preds (list:ProcessorPredicate): List of disjunctive ProcessorPredicates. Defaults to an empty list.

    Note:
        If an instance is composed of both conjuctive and disjunctive predicates, only the conjuctive
        list will be evaluated.

    Examples:
        >>> event = {'source': 'aws.emr', 'detail': {'clusterId': 'j-1YONHTCP3YZKC', 'state': 'COMPLETED'}}
        >>> predicate = ProcessorPredicate('detail.state', lambda state: state == 'COMPLETED')
        >>> predicate.test(event)
        True
    """
    def __init__(self, key, pred_func, and_preds=None, or_preds=None):
        self.key = key
        self.pred_func = pred_func
        self.and_preds = and_preds if and_preds else []
        self.or_preds = or_preds if or_preds else []

    def with_and(self, add_pred):
        """
        Add a predicate to the conjuctive list.

        Args:
            add_pred (ProcessorPredicate): The predicate to add.

        Returns:
            This instance.
        """
        self.and_preds.append(add_pred)
        return self

    def with_or(self, add_pred):
        """
        Add a predicate to the disjunctive list.

        Args:
            add_pred (ProcessorPredicate): The predicate to add.

        Returns:
            This instance.
        """
        self.or_preds.append(add_pred)
        return self

    def test(self, event):
        """
        Evaluate an event.

        Args:
            event (dict): The event to evaluate.

        Returns:
            bool: True if the event satisfies the predicate's conditions, False otherwise.
        """
        test_value = _reduce_to_path(event, self.key)

        result = self.pred_func(test_value) if test_value else False
        if self.and_preds and result:
            for pred in self.and_preds:
                result &= pred.test(event)
                if not result:
                    break
        elif self.or_preds and not result:
            for pred in self.or_preds:
                result |= pred.test(event)
                if result:
                    break
        return result

class EventProcessor(object):
    """
    Validates and extracts the resource id from events.

    Instances of this class will extract the value at the given id path, if the its predicate evaluates to True
    form the event. If an instance does not have a predicate the id will be returned without testing. If either the
    predicate fails or the id path is invalid None will be returned.

    Args:
        source (str): The origination this processor supports events for, e.g. aws.emr
        id_path (str): Dot delineated path to the resource id value.
        predicate (ProcessorPredicate): Prediate to test the event against before extracting the id. Defaults to None.

    Examples:
        >>> event = {'source': 'aws.emr', 'detail': {'clusterId': 'j-1YONHTCP3YZKC', 'state': 'COMPLETED'}}
        >>> predicate = ProcessorPredicate('detail.state', lambda state: state == 'COMPLETED')
        >>> processor = EventProcessor('aws.emr', 'detail.clusterId', predicate=predicate)
        >>> processor.test_and_get_id(event)
        j-1YONHTCP3YZKC
    """
    def __init__(self, source, id_path, predicate=None):
        self.source = source
        self.id_path = id_path
        self.predicate = predicate

    def test_and_get_id(self, event):
        """
        Test the event and extract its resource id.

        If the id path is invalid or the predicate fails None will be returned.

        Args:
            event (dict): Event to test and extract the resource id of.

        Returns:
            str: Id of the resource the event was triggered.
        """
        return None if self.predicate and not self.predicate.test(event) else _reduce_to_path(event, self.id_path)

class EventProcessorManager(object):
    """
    Removes non-fungible tokens from DynamoDB represented by termination events.

    Instances of this class are composed of multiple EventProcessors. Each processor is mapped to
    a single event source, e.g. aws.emr. The processor associated with the event source will verify
    the event is represented by a token and extract the resource id from the event. The resource id
    will then be used to determine the resource coordinate and delete the token.

    Args:
        table_name (str): Name of the DynamoDB non-fungible token table.
                          Can be set via environment variable `NON_FUNG_TABLE`. Defaults to None.
        index_name (str): Name of the index to query for the resource coordinate using the resource id.
                          Can be set via environment variable `NON_FUNG_RES_INDEX`. Defaults to None.
        processors (list:EventProcessor): List of event processors. Defaults to None.

    Examples:
        >>> event = {'source': 'aws.emr', 'detail': {'clusterId': 'j-1YONHTCP3YZKC', 'state': 'COMPLETED'}}
        >>> predicate = ProcessorPredicate('detail.state', lambda state: state == 'COMPLETED')
        >>> processor = EventProcessor('aws.emr', 'detail.clusterId', predicate=predicate)
        >>> manager = manager = EventProcessorManager(table_name='table', index_name='idx', processors=[processor])
        >>> manager.process_event(event)
    """
    def __init__(self, table_name=None, index_name=None, processors=None):
        self.processors = {x.source: x for x in processors} if processors else {}
        self.table_name = _validate_required_env_fallback(table_name, 'table_name', 'NON_FUNG_TABLE')
        self.index_name = _validate_required_env_fallback(index_name, 'index_name', 'NON_FUNG_RES_INDEX')

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
        """
        Add a processor to this manager.

        If a processor already exists for the event source, it will be replaced.

        Args:
            processor (EventProcessor): Processor to include when handling events.
        """
        self.processors[processor.source] = processor

    def process_event(self, event):
        """
        Remove the token which represents the resource contained in the event.

        First, the EventProcessor will be selected according to the event source.
        If no processor is found a ValueError will be raised.

        Next, the processor will test the event and extract the resource id.
        If the event is malformed or does not satisfy the processor's testing criteria nothing will be deleted.

        Finally, the resource id is used to find the resource coordinate. The resource coordinate and id form
        the key needed to delete the token.

        Args:
            event (dict): Event containing the resource to remove the token of.

        Raises:
            ValueError: If the event does not contain 'source' if this manager has no processor for the source.
        """
        processor = self._get_processor(event)
        resource_id = processor.test_and_get_id(event)
        if resource_id:
            resource_coordinate = self._get_resource_coordinate(resource_id)
            if resource_coordinate:
                logger.info('Removing %s token %s from %s', processor.source, resource_id, self.table_name)
                self.table.delete_item(
                    Key={
                        RESOURCE_COORDINATE: resource_coordinate,
                        RESOURCE_ID: resource_id
                    }
                )
            else:
                logger.warn('Could not find a token for resoure %s', resource_id)

    def _get_processor(self, event):
        """
        Get the appropriate processor for this event.

        Args:
            event (dict): Event to processor is being retrieved for.

        Returns:
            EventProcessor: Processor for the event's source.

        Raises:
            ValueError: If the event does not contain 'source' if this manager has no processor for the source.
        """
        if 'source' not in event:
            raise ValueError('Cannot process event, source is a required field. Event: ' + str(event))

        source = event['source']
        processor = self.processors.get(source, None)
        if not processor:
            raise ValueError('No processor for event source: ' + source)
        return processor

    def _get_resource_coordinate(self, resource_id):
        """
        Get the resource coordinate column value for the given resource id.

        Args:
            resource_id (str): Resource id to get the resource coordinate of.

        Returns:
            str: The resource coordinate if a token with the resource id exists, None otherwise.
        """
        response = self.table.query(
            IndexName=self.index_name,
            KeyConditionExpression=Key(RESOURCE_ID).eq(resource_id))
        return None if response['Count'] == 0 else response['Items'][0][RESOURCE_COORDINATE]

def _validate_required_env_fallback(param_value, param_name, env_var):
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
