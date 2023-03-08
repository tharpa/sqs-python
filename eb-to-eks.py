# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Purpose

Read messages from the Amazon Simple Queue Service (Amazon SQS).
"""

import logging

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the service resource
sqs = boto3.resource('sqs')


# snippet-start:[python.example_code.sqs.ReceiveMessage]
def receive_messages(queue, max_number, wait_time):
    """
    Receive a batch of messages in a single request from an SQS queue.

    :param queue: The queue from which to receive messages.
    :param max_number: The maximum number of messages to receive. The actual
                       number of messages received might be less.
    :param wait_time: The maximum time to wait (in seconds) before returning.
                      When this number is greater than zero, long polling is
                      used. This can result in reduced costs and fewer false
                      empty responses.
    :return: The list of Message objects received. These each contain the body
             of the message and metadata and custom attributes.
    """
    try:
        messages = queue.receive_messages(
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=max_number,
            WaitTimeSeconds=wait_time
        )
        for msg in messages:
            logger.info("Received message: %s: %s", msg.message_id, msg.body)
    except ClientError as error:
        logger.exception("Couldn't receive messages from queue: %s", queue)
        raise error
    else:
        return messages
# snippet-end:[python.example_code.sqs.ReceiveMessage]


def eb_to_eks():
    """Read and delete messages from the eb-to-eks-queue."""
    # Get the queue
    queue = sqs.get_queue_by_name(QueueName='eb-to-eks-queue')

    # Process messages by printing out body and optional author name
    for msg in receive_messages(queue, 2, 10):
        # Print out the body of the message
        print('Hello, {0}'.format(msg.body))

        # Let the queue know that the message is processed
        msg.delete()


if __name__ == '__main__':
    eb_to_eks()
