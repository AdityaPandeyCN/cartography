from dataclasses import dataclass

from cartography.models.core.common import PropertyRef
from cartography.models.core.nodes import CartographyNodeProperties
from cartography.models.core.nodes import CartographyNodeSchema
from cartography.models.core.relationships import CartographyRelProperties
from cartography.models.core.relationships import CartographyRelSchema
from cartography.models.core.relationships import LinkDirection
from cartography.models.core.relationships import make_target_node_matcher
from cartography.models.core.relationships import OtherRelationships
from cartography.models.core.relationships import TargetNodeMatcher


@dataclass(frozen=True)
class EventRuleNodeProperties(CartographyNodeProperties):
    """Properties for CloudWatch Event Rule nodes"""

    id: PropertyRef = PropertyRef("Arn")
    arn: PropertyRef = PropertyRef("Arn", extra_index=True)
    lastupdated: PropertyRef = PropertyRef("lastupdated", set_in_kwargs=True)

    name: PropertyRef = PropertyRef("Name", extra_index=True)
    state: PropertyRef = PropertyRef("State")
    description: PropertyRef = PropertyRef("Description")
    event_pattern: PropertyRef = PropertyRef("EventPattern")
    schedule_expression: PropertyRef = PropertyRef("ScheduleExpression")
    role_arn: PropertyRef = PropertyRef("RoleArn")
    event_bus_name: PropertyRef = PropertyRef("EventBusName")
    managed_by: PropertyRef = PropertyRef("ManagedBy")
    created_by: PropertyRef = PropertyRef("CreatedBy")

    region: PropertyRef = PropertyRef("Region", set_in_kwargs=True)


@dataclass(frozen=True)
class _EventRuleRelProperties(CartographyRelProperties):
    lastupdated: PropertyRef = PropertyRef("lastupdated", set_in_kwargs=True)


@dataclass(frozen=True)
class EventRuleToAWSAccountRel(CartographyRelSchema):
    """(:EventRule)<-[:RESOURCE]-(:AWSAccount)"""

    target_node_label: str = "AWSAccount"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"id": PropertyRef("AWS_ID", set_in_kwargs=True)},
    )
    direction: LinkDirection = LinkDirection.INWARD
    rel_label: str = "RESOURCE"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToIAMRoleRel(CartographyRelSchema):
    target_node_label: str = "AWSRole"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("RoleArn")},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "USES_ROLE"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToLambdaFunctionRel(CartographyRelSchema):
    """One-to-many relationship to AWSLambda nodes.

    Note: Lambda functions store their ARN in the 'id' field, so we match
    against 'id' even though the property name suggests ARNs.
    """

    target_node_label: str = "AWSLambda"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {
            "id": PropertyRef("lambda_function_arns", one_to_many=True),
        }
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "TRIGGERS"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToSNSTopicRel(CartographyRelSchema):
    target_node_label: str = "SNSTopic"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("sns_topic_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "PUBLISHES_TO"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToSQSQueueRel(CartographyRelSchema):
    target_node_label: str = "SQSQueue"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("sqs_queue_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "SENDS_TO"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToECSClusterRel(CartographyRelSchema):
    target_node_label: str = "ECSCluster"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("ecs_cluster_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "RUNS_TASK_IN"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToStepFunctionsRel(CartographyRelSchema):
    target_node_label: str = "StepFunction"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("step_functions_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "STARTS_EXECUTION"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToKinesisStreamRel(CartographyRelSchema):
    target_node_label: str = "KinesisStream"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("kinesis_stream_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "PUTS_RECORDS_TO"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToCodeBuildProjectRel(CartographyRelSchema):
    target_node_label: str = "CodeBuildProject"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("codebuild_project_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "TRIGGERS_BUILD"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToCodePipelineRel(CartographyRelSchema):
    target_node_label: str = "CodePipeline"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("codepipeline_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "STARTS_PIPELINE"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToApiGatewayRel(CartographyRelSchema):
    """Link to API Gateway REST APIs.

    Note: API Gateway REST APIs store their ARN in the 'id' field, so we match
    against 'id' even though the property name suggests ARNs.
    """

    target_node_label: str = "APIGatewayRestAPI"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {
            "id": PropertyRef("api_gateway_arns", one_to_many=True),
        }
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "INVOKES_API"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToCloudWatchLogGroupRel(CartographyRelSchema):
    target_node_label: str = "CloudWatchLogGroup"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("cloudwatch_log_group_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "LOGS_TO"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToBatchJobQueueRel(CartographyRelSchema):
    target_node_label: str = "BatchJobQueue"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("batch_job_queue_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "SUBMITS_TO"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToSageMakerPipelineRel(CartographyRelSchema):
    target_node_label: str = "SageMakerPipeline"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("sagemaker_pipeline_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "STARTS_PIPELINE"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToFirehoseDeliveryStreamRel(CartographyRelSchema):
    target_node_label: str = "FirehoseDeliveryStream"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("firehose_delivery_stream_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "DELIVERS_TO"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToRedshiftClusterRel(CartographyRelSchema):
    target_node_label: str = "RedshiftCluster"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("redshift_cluster_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "QUERIES"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleSchema(CartographyNodeSchema):
    """Schema for CloudWatch Event Rules"""

    label: str = "EventRule"
    properties: EventRuleNodeProperties = EventRuleNodeProperties()
    sub_resource_relationship: EventRuleToAWSAccountRel = EventRuleToAWSAccountRel()
    other_relationships: OtherRelationships = OtherRelationships(
        [
            EventRuleToIAMRoleRel(),
            EventRuleToLambdaFunctionRel(),
            EventRuleToSNSTopicRel(),
            EventRuleToSQSQueueRel(),
            EventRuleToECSClusterRel(),
            EventRuleToStepFunctionsRel(),
            EventRuleToKinesisStreamRel(),
            EventRuleToCodeBuildProjectRel(),
            EventRuleToCodePipelineRel(),
            EventRuleToApiGatewayRel(),
            EventRuleToCloudWatchLogGroupRel(),
            EventRuleToBatchJobQueueRel(),
            EventRuleToSageMakerPipelineRel(),
            EventRuleToFirehoseDeliveryStreamRel(),
            EventRuleToRedshiftClusterRel(),
        ]
    )
