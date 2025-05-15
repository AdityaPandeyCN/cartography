import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

import boto3
import neo4j

from cartography.client.core.tx import load
from cartography.graph.job import GraphJob
from cartography.models.core.common import PropertyRef
from cartography.models.core.nodes import CartographyNodeProperties, CartographyNodeSchema
from cartography.models.core.relationships import CartographyRelProperties, CartographyRelSchema, LinkDirection, TargetNodeMatcher, make_target_node_matcher
from cartography.models.core.relationships import OtherRelationships
from cartography.util import timeit, run_cleanup_job, merge_module_sync_metadata
from cartography.stats import get_stats_client

logger = logging.getLogger(__name__)
stat_handler = get_stats_client(__name__)


@dataclass(frozen=True)
class AwsSnsTopicNodeProperties(CartographyNodeProperties):
    id: PropertyRef = PropertyRef('TopicArn')
    arn: PropertyRef = PropertyRef('TopicArn', extra_index=True)
    name: PropertyRef = PropertyRef('TopicName')
    displayname: PropertyRef = PropertyRef('DisplayName')
    owner: PropertyRef = PropertyRef('Owner')
    subscriptionspending: PropertyRef = PropertyRef('SubscriptionsPending')
    subscriptionsconfirmed: PropertyRef = PropertyRef('SubscriptionsConfirmed')
    subscriptionsdeleted: PropertyRef = PropertyRef('SubscriptionsDeleted')
    deliverypolicy: PropertyRef = PropertyRef('DeliveryPolicy')
    effectivedeliverypolicy: PropertyRef = PropertyRef('EffectiveDeliveryPolicy')
    kmsmasterkeyid: PropertyRef = PropertyRef('KmsMasterKeyId')
    region: PropertyRef = PropertyRef('Region', set_in_kwargs=True)
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)


@dataclass(frozen=True)
class AwsSnsTopicToAwsAccountRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef('lastupdated', set_in_kwargs=True)


@dataclass(frozen=True)
class AwsSnsTopicToAWSAccount(CartographyRelSchema):
    target_node_label: str = 'AWSAccount'
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {'id': PropertyRef('AWS_ACCOUNT_ID', set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = "RESOURCE"
    properties: AwsSnsTopicToAwsAccountRelProperties = AwsSnsTopicToAwsAccountRelProperties()


@dataclass(frozen=True)
class AwsSnsTopicSchema(CartographyNodeSchema):
    label: str = 'AwsSnsTopic'
    properties: AwsSnsTopicNodeProperties = AwsSnsTopicNodeProperties()
    sub_resource_relationship: AwsSnsTopicToAWSAccount = AwsSnsTopicToAWSAccount()


@timeit
def get_neo4j_version(neo4j_session: neo4j.Session) -> float:
    """Detect Neo4j version to handle syntax differences."""
    result = neo4j_session.run("CALL dbms.components() YIELD versions RETURN versions[0] as version")
    version_string = result.single()["version"]
    # Parse major.minor version (e.g., "3.5.12" -> 3.5)
    major, minor = version_string.split(".")[:2]
    return float(f"{major}.{minor}")


@timeit
def compatible_load(
    neo4j_session: neo4j.Session,
    schema: CartographyNodeSchema,
    data: List[Dict],
    **kwargs
) -> None:
    """
    A version of the load function that's compatible with Neo4j 3.5.
    This avoids using the 'CREATE INDEX IF NOT EXISTS' syntax.
    """
    neo4j_version = get_neo4j_version(neo4j_session)
    
    # Skip index creation or use compatible syntax for Neo4j 3.5
    if neo4j_version >= 4.0:
        # Use regular load for Neo4j 4.x+
        load(neo4j_session, schema, data, **kwargs)
        return
    
    # For Neo4j 3.5, implement a simplified load logic that avoids the incompatible index syntax
    label = schema.label
    properties = schema.properties
    relationship = schema.sub_resource_relationship
    
    # Create property mappings
    property_map = {}
    for prop_name, prop_ref in properties.__dict__.items():
        if isinstance(prop_ref, PropertyRef):
            if prop_ref.set_in_kwargs:
                # Property comes from kwargs
                if prop_ref.name in kwargs:
                    property_map[prop_name] = prop_ref.name
            else:
                # Property comes from data items
                property_map[prop_name] = prop_ref.name
    
    # Process each data item
    for item in data:
        # Build node properties
        node_props = {}
        for prop_name, src_name in property_map.items():
            if src_name in kwargs:
                node_props[prop_name] = kwargs[src_name]
            elif src_name in item:
                node_props[prop_name] = item[src_name]
        
        # Create or update node
        query = f"""
        MERGE (n:{label} {{id: $id}})
        ON CREATE SET n.firstseen = timestamp()
        SET {', '.join([f'n.{prop} = ${prop}' for prop in node_props])}
        """
        
        neo4j_session.run(query, **node_props)
        
        # For SNS Topics, we know the relationship is with AWSAccount on account_id
        # Directly create relationship with account rather than dynamically processing matchers
        if relationship and isinstance(relationship, AwsSnsTopicToAWSAccount):
            aws_account_id = kwargs.get('AWS_ACCOUNT_ID')
            update_tag = kwargs.get('lastupdated')
            
            if aws_account_id and update_tag:
                # Create relationship to AWS account
                rel_query = f"""
                MATCH (n:{label} {{id: $id}}), (a:AWSAccount {{id: $aws_account_id}})
                MERGE (a)-[r:RESOURCE]->(n)
                ON CREATE SET r.firstseen = timestamp()
                SET r.lastupdated = $update_tag
                """
                
                rel_params = {
                    "id": node_props.get("id"),
                    "aws_account_id": aws_account_id,
                    "update_tag": update_tag
                }
                
                neo4j_session.run(rel_query, **rel_params)


@timeit
def get_sns_topics(boto3_session: boto3.session.Session, region: str) -> List[Dict]:
    """
    Get all SNS Topics for a region.
    """
    client = boto3_session.client('sns', region_name=region)
    paginator = client.get_paginator('list_topics')
    topics = []
    for page in paginator.paginate():
        topics.extend(page.get('Topics', []))
    
    return topics


@timeit
def get_topic_attributes(boto3_session: boto3.session.Session, topic_arn: str, region: str) -> Optional[Dict]:
    """
    Get attributes for an SNS Topic.
    """
    client = boto3_session.client('sns', region_name=region)
    try:
        return client.get_topic_attributes(TopicArn=topic_arn)
    except Exception as e:
        logger.warning(f"Error getting attributes for SNS topic {topic_arn}: {e}")
        return None


@timeit
def transform_sns_topics(topics: List[Dict], attributes: Dict[str, Dict], region: str) -> List[Dict]:
    """
    Transform SNS topic data for ingestion
    """
    transformed_topics = []
    for topic in topics:
        topic_arn = topic['TopicArn']
        
        # Extract topic name from ARN
        # Format: arn:aws:sns:region:account-id:topic-name
        topic_name = topic_arn.split(':')[-1]
        
        # Get attributes
        topic_attrs = attributes.get(topic_arn, {}).get('Attributes', {})
        
        transformed_topic = {
            'TopicArn': topic_arn,
            'TopicName': topic_name,
            'DisplayName': topic_attrs.get('DisplayName', ''),
            'Owner': topic_attrs.get('Owner', ''),
            'SubscriptionsPending': int(topic_attrs.get('SubscriptionsPending', '0')),
            'SubscriptionsConfirmed': int(topic_attrs.get('SubscriptionsConfirmed', '0')),
            'SubscriptionsDeleted': int(topic_attrs.get('SubscriptionsDeleted', '0')),
            'DeliveryPolicy': topic_attrs.get('DeliveryPolicy', ''),
            'EffectiveDeliveryPolicy': topic_attrs.get('EffectiveDeliveryPolicy', ''),
            'KmsMasterKeyId': topic_attrs.get('KmsMasterKeyId', ''),
        }
        
        transformed_topics.append(transformed_topic)
    
    return transformed_topics


@timeit
def load_sns_topics(
    neo4j_session: neo4j.Session, 
    data: List[Dict], 
    region: str, 
    aws_account_id: str, 
    update_tag: int
) -> None:
    """
    Load SNS Topics information into the graph
    """
    logger.info(f"Loading {len(data)} SNS topics for region {region} into graph.")
    
    # Use the compatible_load function instead of regular load
    compatible_load(
        neo4j_session,
        AwsSnsTopicSchema(),
        data,
        lastupdated=update_tag,
        Region=region,
        AWS_ACCOUNT_ID=aws_account_id,
    )


@timeit
def cleanup_sns(neo4j_session: neo4j.Session, common_job_parameters: Dict, test_neo4j_version: Optional[float] = None) -> None:
    """
    Run SNS cleanup job
    """
    logger.debug("Running SNS cleanup job.")
    
    # Use provided version for testing or detect it
    neo4j_version = test_neo4j_version if test_neo4j_version is not None else get_neo4j_version(neo4j_session)
    
    if neo4j_version >= 4.0:
        # Use regular GraphJob for Neo4j 4.x+
        cleanup_job = GraphJob.from_node_schema(AwsSnsTopicSchema(), common_job_parameters)
        cleanup_job.run(neo4j_session)
    else:
        # Direct query implementation for Neo4j 3.5
        update_tag = common_job_parameters.get('UPDATE_TAG')
        aws_id = common_job_parameters.get('AWS_ID')
        
        # Delete nodes that weren't updated
        neo4j_session.run(
            """
            MATCH (n:AwsSnsTopic)
            WHERE n.lastupdated <> $update_tag
            WITH n LIMIT 10000
            DETACH DELETE n
            """,
            update_tag=update_tag
        )


@timeit
def sync_sns_topics(
    neo4j_session: neo4j.Session, 
    boto3_session: boto3.session.Session, 
    regions: List[str], 
    current_aws_account_id: str, 
    update_tag: int,
    common_job_parameters: Dict
) -> None:
    """
    Sync SNS Topics for all regions
    """
    for region in regions:
        logger.info(f"Syncing SNS Topics for {region} in account {current_aws_account_id}")
        topics = get_sns_topics(boto3_session, region)
        
        # Get attributes for each topic
        topic_attributes = {}
        for topic in topics:
            topic_arn = topic['TopicArn']
            attrs = get_topic_attributes(boto3_session, topic_arn, region)
            if attrs:
                topic_attributes[topic_arn] = attrs
        
        # Transform data
        transformed_topics = transform_sns_topics(topics, topic_attributes, region)
        
        # Load data
        load_sns_topics(
            neo4j_session, 
            transformed_topics, 
            region, 
            current_aws_account_id, 
            update_tag
        )
    
    # Clean up old entries
    cleanup_sns(neo4j_session, common_job_parameters)
    
    # Record that we've synced this module
    merge_module_sync_metadata(
        neo4j_session,
        group_type='AWSAccount',
        group_id=current_aws_account_id,
        synced_type='AwsSnsTopic',
        update_tag=update_tag,
        stat_handler=stat_handler
    )