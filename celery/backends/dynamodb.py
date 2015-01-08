# -*- coding: utf-8 -*-
"""
    celery.backends.dynamodb
    ~~~~~~~~~~~~~~~~~~~~~~~~

    DynamoDB result store backend.
"""
from __future__ import absolute_import

from celery.utils.log import get_logger
from celery.exceptions import ImproperlyConfigured

from .base import KeyValueStoreBackend

try:
    import boto
    from boto import dynamodb2
    from boto.dynamodb2.layer1 import DynamoDBConnection
    from boto.dynamodb2.table import Table
    from boto.dynamodb2.fields import HashKey
    from boto.dynamodb2.exceptions import ItemNotFound
    from boto.dynamodb.types import Binary
except ImportError:  # pragma: no cover
    boto = dynamodb2 = DynamoDBConnection = Table = HashKey = Binary = ItemNotFound = Binary = None  # noqa

BOTO_MISSING = """\
You need to install the boto library in order to use \
the DynamoDB result store backend."""

logger = get_logger(__name__)
error = logger.error


class DynamoBackend(KeyValueStoreBackend):
    """DynamoDB task result store.  Implements the interface for
    KeyValueStoreBackend."""

    dynamodb2 = dynamodb2
    Table = Table

    def __init__(self, tablename=None, aws_access_key=None,
                 aws_secret_access_key=None, region=None, local=False,
                 **kwargs):
        super(DynamoBackend, self).__init__(**kwargs)
        if self.dynamodb2 is None or self.Table is None:
            raise ImproperlyConfigured(BOTO_MISSING)

        conf = self.app.conf

        self.tablename = tablename or conf['CELERY_DYNAMODB_TABLE']
        self.aws_access_key_id = aws_access_key or conf.get('CELERY_DYNAMODB_ACCESS_KEY')
        self.aws_secret_access_key = aws_secret_access_key or conf.get('CELERY_DYNAMODB_SECRET_KEY')
        self.region = region or conf.get('CELERY_DYNAMODB_REGION')

        # NOTE: If any of the config options above other than tablename are
        # None, boto will use environment variables and config files to find
        # credentials.
        self.conn = self.dynamodb2.connect_to_region(
            self.region,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

        self.table = self.Table(self.tablename, connection=self.conn)

    def get(self, key):
        try:
            return self.table.get_item(k=key)['v'].value
        except ItemNotFound:
            return None

    def set(self, key, value):
        self.table.put_item({'k': key, 'v': Binary(value)}, overwrite=True)

    def mget(self, keys):
        keys = [{'k': k} for k in keys]
        results = self.table.batch_get(keys=keys)
        return [r['v'].value for r in results]

    def delete(self, key):
        try:
            item = self.table.get_item(key)
            item.delete()
        except ItemNotFound:
            # The item is already deleted.
            pass
