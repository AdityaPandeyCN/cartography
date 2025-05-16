import json

# Mock data similar to the structure used for S3 test data
LIST_TOPICS = {
    "Topics": [
        {
            "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic-1"
        },
        {
            "TopicArn": "arn:aws:sns:us-west-1:123456789012:test-topic-2"
        },
        {
            "TopicArn": "arn:aws:sns:eu-west-1:123456789012:test-topic-3"
        }
    ]
}

# Mock data for topic attributes
GET_TOPIC_ATTRIBUTES = {
    "Attributes": {
        "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic",
        "Owner": "123456789012",
        "DisplayName": "Test Topic",
        "SubscriptionsPending": "0",
        "SubscriptionsConfirmed": "1",
        "SubscriptionsDeleted": "0",
        "DeliveryPolicy": "{}",
        "EffectiveDeliveryPolicy": "{}",
        "KmsMasterKeyId": "arn:aws:kms:us-east-1:123456789012:key/test-key",
    }
}

# Pre-transformed data for easier testing
TRANSFORMED_TOPICS = [
    {
        'TopicArn': 'arn:aws:sns:us-east-1:123456789012:test-topic-1',
        'TopicName': 'test-topic-1',
        'DisplayName': 'Test Topic 1',
        'Owner': '123456789012',
        'SubscriptionsPending': 0,
        'SubscriptionsConfirmed': 2,
        'SubscriptionsDeleted': 0,
        'DeliveryPolicy': '',
        'EffectiveDeliveryPolicy': json.dumps({
            "http": {
                "defaultHealthyRetryPolicy": {
                    "minDelayTarget": 20,
                    "maxDelayTarget": 20,
                    "numRetries": 3,
                    "numMaxDelayRetries": 0,
                    "numNoDelayRetries": 0,
                    "numMinDelayRetries": 0,
                    "backoffFunction": "linear"
                },
                "disableSubscriptionOverrides": False,
                "defaultThrottlePolicy": {
                    "maxReceivesPerSecond": 1
                }
            }
        }),
        'KmsMasterKeyId': '',
        'Region': 'us-east-1',
    },
    {
        'TopicArn': 'arn:aws:sns:us-west-1:123456789012:test-topic-2',
        'TopicName': 'test-topic-2',
        'DisplayName': 'Test Topic 2',
        'Owner': '123456789012',
        'SubscriptionsPending': 1,
        'SubscriptionsConfirmed': 5,
        'SubscriptionsDeleted': 2,
        'DeliveryPolicy': '',
        'EffectiveDeliveryPolicy': json.dumps({
            "http": {
                "defaultHealthyRetryPolicy": {
                    "minDelayTarget": 20,
                    "maxDelayTarget": 20,
                    "numRetries": 3,
                    "numMaxDelayRetries": 0,
                    "numNoDelayRetries": 0,
                    "numMinDelayRetries": 0,
                    "backoffFunction": "linear"
                },
                "disableSubscriptionOverrides": False,
                "defaultThrottlePolicy": {
                    "maxReceivesPerSecond": 1
                }
            }
        }),
        'KmsMasterKeyId': 'arn:aws:kms:us-west-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab',
        'Region': 'us-west-1',
    },
    {
        'TopicArn': 'arn:aws:sns:eu-west-1:123456789012:test-topic-3',
        'TopicName': 'test-topic-3',
        'DisplayName': 'Test Topic 3',
        'Owner': '123456789012',
        'SubscriptionsPending': 0,
        'SubscriptionsConfirmed': 10,
        'SubscriptionsDeleted': 0,
        'DeliveryPolicy': json.dumps({
            "http": {
                "defaultHealthyRetryPolicy": {
                    "minDelayTarget": 10,
                    "maxDelayTarget": 30,
                    "numRetries": 5
                }
            }
        }),
        'EffectiveDeliveryPolicy': json.dumps({
            "http": {
                "defaultHealthyRetryPolicy": {
                    "minDelayTarget": 10,
                    "maxDelayTarget": 30,
                    "numRetries": 5,
                    "numMaxDelayRetries": 0,
                    "numNoDelayRetries": 0,
                    "numMinDelayRetries": 0,
                    "backoffFunction": "linear"
                },
                "disableSubscriptionOverrides": False,
                "defaultThrottlePolicy": {
                    "maxReceivesPerSecond": 1
                }
            }
        }),
        'KmsMasterKeyId': '',
        'Region': 'eu-west-1',
    },
]

# List of SNS topics in a simpler format
LIST_SNS_TOPICS = [
    {
        "TopicArn": "arn:aws:sns:us-east-1:123456789012:test-topic",
        "Name": "test-topic",
    }
]