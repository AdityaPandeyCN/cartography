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

    codebuild_project_arns: PropertyRef = PropertyRef("codebuild_project_arns")
    codepipeline_arns: PropertyRef = PropertyRef("codepipeline_arns")
    api_gateway_arns: PropertyRef = PropertyRef("api_gateway_arns")

    unknown_target_arns: PropertyRef = PropertyRef("unknown_target_arns")

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
    """(:EventRule)-[:USES_ROLE]->(:AWSRole)

    Important for security analysis - shows which role the rule assumes
    when invoking targets.
    """

    target_node_label: str = "AWSRole"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("RoleArn")},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "USES_ROLE"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToLambdaFunctionRel(CartographyRelSchema):
    """(:EventRule)-[:TRIGGERS]->(:AWSLambda)

    Most common EventBridge target - Lambda functions.
    Note: Lambda functions store their ARN in the 'id' field.
    """

    target_node_label: str = "AWSLambda"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"id": PropertyRef("lambda_function_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "TRIGGERS"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToSNSTopicRel(CartographyRelSchema):
    """(:EventRule)-[:PUBLISHES_TO]->(:SNSTopic)

    Second most common target - SNS topics for notifications.
    """

    target_node_label: str = "SNSTopic"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("sns_topic_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "PUBLISHES_TO"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToSQSQueueRel(CartographyRelSchema):
    """(:EventRule)-[:SENDS_TO]->(:SQSQueue)

    Third most common target - SQS queues for async processing.
    """

    target_node_label: str = "SQSQueue"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("sqs_queue_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "SENDS_TO"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToCodeBuildProjectRel(CartographyRelSchema):
    """(:EventRule)-[:TRIGGERS_BUILD]->(:CodeBuildProject)"""

    target_node_label: str = "CodeBuildProject"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("codebuild_project_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "TRIGGERS_BUILD"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToCodePipelineRel(CartographyRelSchema):
    """(:EventRule)-[:STARTS_PIPELINE]->(:CodePipeline)"""

    target_node_label: str = "CodePipeline"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"arn": PropertyRef("codepipeline_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "STARTS_PIPELINE"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleToAPIGatewayRel(CartographyRelSchema):
    """(:EventRule)-[:INVOKES_API]->(:APIGatewayRestAPI)"""

    target_node_label: str = "APIGatewayRestAPI"
    target_node_matcher: TargetNodeMatcher = make_target_node_matcher(
        {"id": PropertyRef("api_gateway_arns", one_to_many=True)},
    )
    direction: LinkDirection = LinkDirection.OUTWARD
    rel_label: str = "INVOKES_API"
    properties: _EventRuleRelProperties = _EventRuleRelProperties()


@dataclass(frozen=True)
class EventRuleSchema(CartographyNodeSchema):
    """Schema for CloudWatch Event Rules.

    This schema focuses on the most common use cases (Lambda, SNS, SQS)
    which cover ~80% of real-world EventBridge usage. Other target types
    are captured in the unknown_target_arns property for future analysis.
    """

    label: str = "EventRule"
    properties: EventRuleNodeProperties = EventRuleNodeProperties()
    sub_resource_relationship: EventRuleToAWSAccountRel = EventRuleToAWSAccountRel()
    other_relationships: OtherRelationships = OtherRelationships(
        [
            EventRuleToIAMRoleRel(),
            EventRuleToLambdaFunctionRel(),
            EventRuleToSNSTopicRel(),
            EventRuleToSQSQueueRel(),
            EventRuleToCodeBuildProjectRel(),
            EventRuleToCodePipelineRel(),
            EventRuleToAPIGatewayRel(),
        ]
    )
