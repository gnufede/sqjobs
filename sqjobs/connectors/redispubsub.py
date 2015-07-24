from redis import StrictRedis
import time
from datetime import datetime

import json
from .base import Connector

import logging
logger = logging.getLogger('sqjobs.redis')


class RedisPubSub(Connector):
    """
    Manages a single connection to Redis PUBSUB
    """
    def __init__(self, url):
        """
        Creates a new Redis client object

        :param url: Redis server url

        """
        self.server_url = url
        self.subscription = None
        self._redis_client = None

    def __repr__(self):
        return 'RedisPubSub("{url}")'.format(
            url=self.server_url
        )

    @property
    def connection(self):
        """
        Creates (and saves in a cache) a Redis PubSub client connection
        """
        if self._redis_client is None:
            self._redis_client = StrictRedis.from_url(self.server_url)
            self._redis_client.this_pubsub = self._redis_client.pubsub(ignore_subscribe_messages=True)

            logger.debug('Created new Redis connection')

        return self._redis_client

    def ensure_subscription(self, queue_name):
        if not self.subscription or self.subscription != queue_name:
            if self.subscription:
                self.connection.this_pubsub.unsubscribe(self.subscription)
            self.connection.this_pubsub.subscribe(queue_name)

    def get_queue(self, name):
        self.ensure_subscription(name)

    def get_queues(self):
        return [self.subscription,]

    def get_dead_letter_queues(self):
        return []

    def enqueue(self, queue_name, payload):
        """
        Publishes a new message to a queue

        :param queue_name: the name of the queue
        :param payload: the payload to send inside the message
        """
        self.ensure_subscription(queue_name)

        message_id = str(time.time())
        message = {
            'id': message_id,
            'payload': payload,
            'created_on': str(time.time()),
            'first_execution_on': str(time.time()),
        }
        StrictRedis.publish(self.connection, queue_name, json.dumps(message))

        logger.info('Sent new message to %s', queue_name)
        return message

    def dequeue(self, queue_name, wait_time=20):
        """
        Retrieves a new message from a queue

        :param queue_name: the queue name
        :param wait_time: how much time to wait until a new message is
        retrieved (long polling). If set to zero, connection will return
        inmediately if no messages exist.
        """
        self.ensure_subscription(queue_name)

        while True:
            message = self.connection.this_pubsub.get_message()

            if message:
                data = json.loads(message['data'])
                payload = json.loads(message['data'])['payload']
                payload['_metadata'] = {
                    'id': data['id'],
                    'retries': 0,
                    'created_on': datetime.fromtimestamp(float(data['created_on'])/1000),
                    'first_execution_on': datetime.fromtimestamp(float(data['first_execution_on'])/1000),
                }
                return payload

            if wait_time:
                time.sleep(wait_time)
            else:
                break

    # TODO
    def set_retry_time(self, queue_name, message_id, delay):
        return None

    # TODO
    def delete(self, queue_name, message_id):
        return None

