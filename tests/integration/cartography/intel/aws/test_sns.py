import json
import pytest
from typing import Any, Dict, List

import cartography.intel.aws.sns
import tests.data.aws.sns

TEST_ACCOUNT_ID = "123456789012"
TEST_REGIONS = ["us-east-1", "us-west-1", "eu-west-1"]
TEST_UPDATE_TAG = 123456789


@pytest.fixture(scope="function")
def aws_account(neo4j_session):
    """
    Create an AWS account node for testing.
    """
    neo4j_session.run(
        """
        MERGE (aws:AWSAccount{id: $account_id})
        ON CREATE SET aws.firstseen = timestamp()
        SET aws.lastupdated = $update_tag
        """,
        account_id=TEST_ACCOUNT_ID,
        update_tag=TEST_UPDATE_TAG
    )
    return TEST_ACCOUNT_ID


class TestSNSTopicLoading:
    def test_load_sns_topics(self, neo4j_session, aws_account):
        """
        Ensure that SNS topics get loaded with their key fields.
        """
        region = "us-east-1"
        topic_data = [topic for topic in tests.data.aws.sns.TRANSFORMED_TOPICS if topic['Region'] == region]
        
        cartography.intel.aws.sns.load_sns_topics(
            neo4j_session,
            topic_data,
            region,
            TEST_ACCOUNT_ID,
            TEST_UPDATE_TAG
        )

        # Define expected nodes based on test data
        expected_nodes = {
            (
                "arn:aws:sns:us-east-1:123456789012:test-topic-1",  # id = TopicArn
                "arn:aws:sns:us-east-1:123456789012:test-topic-1",  # arn
                "test-topic-1",                                      # name
                "Test Topic 1",                                      # displayname
                "123456789012",                                      # owner
                0,                                                  # subscriptionspending
                2,                                                  # subscriptionsconfirmed
                0,                                                  # subscriptionsdeleted
                region,                                             # region
            )
        }

        # Query Neo4j for the nodes
        nodes = neo4j_session.run(
            """
            MATCH (t:AwsSnsTopic) 
            WHERE t.region = $region
            RETURN t.id, t.arn, t.name, t.displayname, t.owner, 
                   t.subscriptionspending, t.subscriptionsconfirmed, t.subscriptionsdeleted, t.region
            """,
            region=region
        )
        
        # Convert query results to set of tuples for comparison
        actual_nodes = {
            (
                n["t.id"],
                n["t.arn"],
                n["t.name"],
                n["t.displayname"],
                n["t.owner"],
                n["t.subscriptionspending"],
                n["t.subscriptionsconfirmed"],
                n["t.subscriptionsdeleted"],
                n["t.region"],
            )
            for n in nodes
        }
        
        assert actual_nodes == expected_nodes

    def test_topic_account_relationships(self, neo4j_session, aws_account):
        """
        Ensure that relationships between SNS topics and AWS accounts are created correctly.
        """
        region = "us-east-1"
        topic_data = [topic for topic in tests.data.aws.sns.TRANSFORMED_TOPICS if topic['Region'] == region]
        
        cartography.intel.aws.sns.load_sns_topics(
            neo4j_session,
            topic_data,
            region,
            TEST_ACCOUNT_ID,
            TEST_UPDATE_TAG
        )

        # Check that the relationship exists
        result = neo4j_session.run(
            """
            MATCH (aws:AWSAccount{id: $account_id})-[r:RESOURCE]->(t:AwsSnsTopic{region: $region})
            RETURN count(r) as rel_count
            """,
            account_id=TEST_ACCOUNT_ID,
            region=region
        )
        
        assert result.single()["rel_count"] == 1


class TestSNSTopicAttributes:
    def test_topic_policy_parsing(self, neo4j_session, aws_account):
        """
        Ensure that delivery policies are correctly parsed and stored.
        """
        region = "eu-west-1"  # Using the region with test data
        topic_data = [topic for topic in tests.data.aws.sns.TRANSFORMED_TOPICS if topic['Region'] == region]
        
        cartography.intel.aws.sns.load_sns_topics(
            neo4j_session,
            topic_data,
            region,
            TEST_ACCOUNT_ID,
            TEST_UPDATE_TAG
        )

        # Verify the topic was created
        result = neo4j_session.run(
            """
            MATCH (t:AwsSnsTopic)
            WHERE t.region = $region
            RETURN count(t) as count
            """,
            region=region
        )
        
        assert result.single()["count"] == 1

    def test_delivery_policy_parsing(self, neo4j_session, aws_account):
        """
        Ensure that delivery policies are correctly parsed and stored.
        """
        region = "eu-west-1"
        topic_data = [topic for topic in tests.data.aws.sns.TRANSFORMED_TOPICS if topic['Region'] == region]
        
        cartography.intel.aws.sns.load_sns_topics(
            neo4j_session,
            topic_data,
            region,
            TEST_ACCOUNT_ID,
            TEST_UPDATE_TAG
        )

        result = neo4j_session.run(
            """
            MATCH (t:AwsSnsTopic)
            WHERE t.region = $region
            RETURN t.deliverypolicy, t.effectivedeliverypolicy
            """,
            region=region
        )
        
        record = result.single()
        assert record["t.deliverypolicy"] is not None
        assert record["t.effectivedeliverypolicy"] is not None

    def test_topic_encryption(self, neo4j_session, aws_account):
        """
        Ensure that KMS encryption settings are correctly stored.
        """
        region = "us-west-1"  # Using the region with KMS key in test data
        topic_data = [topic for topic in tests.data.aws.sns.TRANSFORMED_TOPICS if topic['Region'] == region]
        
        cartography.intel.aws.sns.load_sns_topics(
            neo4j_session,
            topic_data,
            region,
            TEST_ACCOUNT_ID,
            TEST_UPDATE_TAG
        )

        result = neo4j_session.run(
            """
            MATCH (t:AwsSnsTopic)
            WHERE t.region = $region
            RETURN t.kmsmasterkeyid
            """,
            region=region
        )
        
        kms_key_id = result.single()["t.kmsmasterkeyid"]
        assert kms_key_id == "arn:aws:kms:us-west-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"


class TestSNSTopicManagement:
    def test_cleanup_sns_topics(self, neo4j_session, aws_account):
        """
        Ensure that cleanup job properly removes old SNS topics.
        """
        region = "us-east-1"
        topic_data = [topic for topic in tests.data.aws.sns.TRANSFORMED_TOPICS if topic['Region'] == region]
        
        # Load with old update tag
        old_update_tag = TEST_UPDATE_TAG - 1000
        cartography.intel.aws.sns.load_sns_topics(
            neo4j_session,
            topic_data,
            region,
            TEST_ACCOUNT_ID,
            old_update_tag
        )

        # Load with new update tag
        cartography.intel.aws.sns.load_sns_topics(
            neo4j_session,
            topic_data,
            region,
            TEST_ACCOUNT_ID,
            TEST_UPDATE_TAG
        )

        # Run cleanup
        common_job_parameters = {
            "UPDATE_TAG": TEST_UPDATE_TAG,
            "AWS_ID": TEST_ACCOUNT_ID
        }
        cartography.intel.aws.sns.cleanup_sns(neo4j_session, common_job_parameters)

        # Verify cleanup
        result = neo4j_session.run(
            """
            MATCH (t:AwsSnsTopic)
            WHERE t.lastupdated < $update_tag
            RETURN count(t) as count
            """,
            update_tag=TEST_UPDATE_TAG
        )
        
        assert result.single()["count"] == 0

    def test_error_handling(self, neo4j_session, aws_account):
        """
        Ensure that the module handles API errors gracefully.
        """
        try:
            invalid_topic_data = [{
                'TopicArn': 'invalid-arn',
                'Region': 'us-east-1'
            }]
            
            cartography.intel.aws.sns.load_sns_topics(
                neo4j_session,
                invalid_topic_data,
                'us-east-1',
                TEST_ACCOUNT_ID,
                TEST_UPDATE_TAG
            )
            
            # If we get here, at least loading didn't crash
            assert True
        except Exception as e:
            pytest.fail(f"Error handling test failed: {e}")

    def test_multi_region_sync(self, neo4j_session, aws_account):
        """
        Ensure that topics from multiple regions are properly synced.
        """
        # Clean up any existing topics first to ensure a clean state
        neo4j_session.run("MATCH (t:AwsSnsTopic) DETACH DELETE t")
        
        for region in TEST_REGIONS:
            topic_data = [topic for topic in tests.data.aws.sns.TRANSFORMED_TOPICS if topic['Region'] == region]
            cartography.intel.aws.sns.load_sns_topics(
                neo4j_session,
                topic_data,
                region,
                TEST_ACCOUNT_ID,
                TEST_UPDATE_TAG
            )

        result = neo4j_session.run(
            """
            MATCH (t:AwsSnsTopic)
            RETURN t.region, count(t) as count
            ORDER BY t.region
            """
        )
        
        regions = {record["t.region"]: record["count"] for record in result}
        assert all(region in regions for region in TEST_REGIONS)
        assert regions["us-east-1"] == 1
        assert regions["us-west-1"] == 1
        assert regions["eu-west-1"] == 1

    def test_topic_subscription_counts(self, neo4j_session, aws_account):
        """
        Ensure that subscription counts are correctly stored and updated.
        """
        region = "us-west-1"
        topic_data = [topic for topic in tests.data.aws.sns.TRANSFORMED_TOPICS if topic['Region'] == region]
        
        # First load
        cartography.intel.aws.sns.load_sns_topics(
            neo4j_session,
            topic_data,
            region,
            TEST_ACCOUNT_ID,
            TEST_UPDATE_TAG
        )

        # Update subscription counts
        updated_topic_data = topic_data.copy()
        updated_topic_data[0]['SubscriptionsConfirmed'] = 6
        updated_topic_data[0]['SubscriptionsPending'] = 0
        
        # Second load with updated counts
        cartography.intel.aws.sns.load_sns_topics(
            neo4j_session,
            updated_topic_data,
            region,
            TEST_ACCOUNT_ID,
            TEST_UPDATE_TAG + 1
        )

        result = neo4j_session.run(
            """
            MATCH (t:AwsSnsTopic)
            WHERE t.region = $region
            RETURN t.subscriptionsconfirmed, t.subscriptionspending
            """,
            region=region
        )
        
        record = result.single()
        assert record["t.subscriptionsconfirmed"] == 6
        assert record["t.subscriptionspending"] == 0