import json
import unittest
from unittest.mock import MagicMock, patch

import boto3
import botocore
from botocore.exceptions import ClientError

import cartography.intel.aws.sns
import tests.data.aws.sns  # Changed from cartography.tests.data.aws.sns


class TestSNSTransform(unittest.TestCase):
    def test_transform_sns_topics(self):
        """
        Test that transform_sns_topics correctly transforms raw SNS topic data.
        """
        # Test data
        topics = tests.data.aws.sns.LIST_TOPICS['Topics']  # Changed
        attributes = tests.data.aws.sns.GET_TOPIC_ATTRIBUTES  # Changed
        region = 'us-east-1'

        # Call the function
        result = cartography.intel.aws.sns.transform_sns_topics(topics, attributes, region)

        # Verify the result
        self.assertEqual(len(result), len(topics))
        
        # Check first topic transformation
        first_topic = result[0]
        self.assertEqual(first_topic['TopicArn'], 'arn:aws:sns:us-east-1:123456789012:test-topic-1')
        self.assertEqual(first_topic['TopicName'], 'test-topic-1')
        self.assertEqual(first_topic['DisplayName'], 'Test Topic 1')
        self.assertEqual(first_topic['Owner'], '123456789012')
        self.assertEqual(first_topic['SubscriptionsPending'], 0)
        self.assertEqual(first_topic['SubscriptionsConfirmed'], 2)
        self.assertEqual(first_topic['SubscriptionsDeleted'], 0)

    def test_transform_sns_topics_with_missing_attributes(self):
        """
        Test that transform_sns_topics handles missing attributes gracefully.
        """
        topics = [{'TopicArn': 'arn:aws:sns:us-east-1:123456789012:missing-topic'}]
        attributes = {}
        region = 'us-east-1'

        result = cartography.intel.aws.sns.transform_sns_topics(topics, attributes, region)

        self.assertEqual(len(result), 1)
        topic = result[0]
        self.assertEqual(topic['TopicArn'], 'arn:aws:sns:us-east-1:123456789012:missing-topic')
        self.assertEqual(topic['TopicName'], 'missing-topic')
        self.assertEqual(topic['DisplayName'], '')
        self.assertEqual(topic['Owner'], '')
        self.assertEqual(topic['SubscriptionsPending'], 0)
        self.assertEqual(topic['SubscriptionsConfirmed'], 0)
        self.assertEqual(topic['SubscriptionsDeleted'], 0)


class TestSNSGet(unittest.TestCase):
    def setUp(self):
        self.session = MagicMock()
        self.client = MagicMock()
        self.session.client.return_value = self.client

    def test_get_sns_topics(self):
        """
        Test that get_sns_topics correctly retrieves and paginates SNS topics.
        """
        # Mock the paginator
        paginator = MagicMock()
        self.client.get_paginator.return_value = paginator
        
        # Mock the paginate results
        paginator.paginate.return_value = [
            {'Topics': [{'TopicArn': 'arn:aws:sns:us-east-1:123456789012:topic1'}]},
            {'Topics': [{'TopicArn': 'arn:aws:sns:us-east-1:123456789012:topic2'}]}
        ]

        result = cartography.intel.aws.sns.get_sns_topics(self.session, 'us-east-1')

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['TopicArn'], 'arn:aws:sns:us-east-1:123456789012:topic1')
        self.assertEqual(result[1]['TopicArn'], 'arn:aws:sns:us-east-1:123456789012:topic2')

    def test_get_topic_attributes_success(self):
        """
        Test that get_topic_attributes correctly retrieves topic attributes.
        """
        topic_arn = 'arn:aws:sns:us-east-1:123456789012:test-topic-1'
        expected_attributes = tests.data.aws.sns.GET_TOPIC_ATTRIBUTES[topic_arn]  # Changed
        
        self.client.get_topic_attributes.return_value = expected_attributes

        result = cartography.intel.aws.sns.get_topic_attributes(self.session, topic_arn, 'us-east-1')

        self.assertEqual(result, expected_attributes)
        self.client.get_topic_attributes.assert_called_once_with(TopicArn=topic_arn)

    def test_get_topic_attributes_error(self):
        """
        Test that get_topic_attributes handles errors gracefully.
        """
        topic_arn = 'arn:aws:sns:us-east-1:123456789012:error-topic'
        
        # Mock a ClientError
        self.client.get_topic_attributes.side_effect = ClientError(
            {'Error': {'Code': 'NotFound', 'Message': 'Topic not found'}},
            'GetTopicAttributes'
        )

        result = cartography.intel.aws.sns.get_topic_attributes(self.session, topic_arn, 'us-east-1')

        self.assertIsNone(result)
        self.client.get_topic_attributes.assert_called_once_with(TopicArn=topic_arn)


class TestSNSCleanup(unittest.TestCase):
    def test_cleanup_sns(self):
        """
        Test that cleanup_sns correctly calls the cleanup job.
        """
        neo4j_session = MagicMock()
        common_job_parameters = {
            'UPDATE_TAG': 123456789,
            'AWS_ID': '123456789012'
        }

        # Call with test version parameter to bypass version detection
        cartography.intel.aws.sns.cleanup_sns(neo4j_session, common_job_parameters, test_neo4j_version=3.5)
        
        # Verify that a cleanup query was run
        neo4j_session.run.assert_called_once()