import logging
from typing import Any

import boto3
import neo4j

from cartography.client.core.tx import load
from cartography.graph.job import GraphJob
from cartography.intel.aws.ec2.util import get_botocore_config
from cartography.models.aws.eventbridge.event_rule import EventRuleSchema
from cartography.stats import get_stats_client
from cartography.util import aws_handle_regions
from cartography.util import merge_module_sync_metadata
from cartography.util import timeit

logger = logging.getLogger(__name__)
stat_handler = get_stats_client(__name__)


DEFAULT_EVENT_BUS = "default"


@timeit
@aws_handle_regions
def get_event_rules(
    boto3_session: boto3.session.Session,
    region: str,
) -> dict[str, Any]:
    """Fetch EventBridge / CloudWatch Event rules together with their targets."""
    client = boto3_session.client(
        "events", region_name=region, config=get_botocore_config()
    )

    paginator = client.get_paginator("list_rules")
    rules: list[dict[str, Any]] = []
    targets_by_rule: dict[str, list[dict[str, Any]]] = {}

    for page in paginator.paginate():
        rules.extend(page.get("Rules", []))

    for rule in rules:
        target_paginator = client.get_paginator("list_targets_by_rule")
        targets: list[dict[str, Any]] = []
        for t_page in target_paginator.paginate(Rule=rule["Name"]):
            targets.extend(t_page.get("Targets", []))
        targets_by_rule[rule["Name"]] = targets

    return {"Rules": rules, "Targets": targets_by_rule}


def transform_event_rules(data: dict[str, Any], region: str) -> list[dict[str, Any]]:
    """Shape the raw AWS API response so it lines up with EventRuleSchema."""
    transformed: list[dict[str, Any]] = []

    for rule in data["Rules"]:
        rule_name: str = rule["Name"]
        targets: list[dict[str, Any]] = data["Targets"].get(rule_name, [])

        lambda_function_arns: list[str] = []
        sns_topic_arns: list[str] = []
        sqs_queue_arns: list[str] = []
        ecs_cluster_arns: list[str] = []
        step_functions_arns: list[str] = []
        kinesis_stream_arns: list[str] = []
        codebuild_project_arns: list[str] = []
        codepipeline_arns: list[str] = []
        api_gateway_arns: list[str] = []
        cloudwatch_log_group_arns: list[str] = []
        batch_job_queue_arns: list[str] = []
        sagemaker_pipeline_arns: list[str] = []
        firehose_delivery_stream_arns: list[str] = []
        redshift_cluster_arns: list[str] = []

        for target in targets:
            target_arn: str = target.get("Arn", "")

            if ":lambda:" in target_arn and ":function:" in target_arn:
                lambda_function_arns.append(target_arn)
            elif ":sns:" in target_arn:
                sns_topic_arns.append(target_arn)
            elif ":sqs:" in target_arn:
                sqs_queue_arns.append(target_arn)
            elif ":ecs:" in target_arn and "cluster/" in target_arn:
                ecs_cluster_arns.append(target_arn)
            elif ":states:" in target_arn:
                step_functions_arns.append(target_arn)
            elif ":kinesis:" in target_arn and ":stream/" in target_arn:
                kinesis_stream_arns.append(target_arn)
            elif ":codebuild:" in target_arn and ":project/" in target_arn:
                codebuild_project_arns.append(target_arn)
            elif ":codepipeline:" in target_arn:
                codepipeline_arns.append(target_arn)
            elif ":execute-api:" in target_arn:
                api_gateway_arns.append(target_arn)
            elif ":logs:" in target_arn and ":log-group:" in target_arn:
                cloudwatch_log_group_arns.append(target_arn)
            elif ":batch:" in target_arn and ":job-queue/" in target_arn:
                batch_job_queue_arns.append(target_arn)
            elif ":sagemaker:" in target_arn and ":pipeline/" in target_arn:
                sagemaker_pipeline_arns.append(target_arn)
            elif ":firehose:" in target_arn and ":deliverystream/" in target_arn:
                firehose_delivery_stream_arns.append(target_arn)
            elif ":redshift:" in target_arn and ":cluster:" in target_arn:
                redshift_cluster_arns.append(target_arn)
            else:
                logger.debug(f"Unknown target type for ARN: {target_arn}")

        item: dict[str, Any] = {
            "Arn": rule["Arn"],
            "Name": rule["Name"],
            "State": rule.get("State"),
            "Description": rule.get("Description"),
            "EventPattern": rule.get("EventPattern"),
            "ScheduleExpression": rule.get("ScheduleExpression"),
            "RoleArn": rule.get("RoleArn"),
            "EventBusName": rule.get("EventBusName", DEFAULT_EVENT_BUS),
            "ManagedBy": rule.get("ManagedBy"),
            "CreatedBy": rule.get("CreatedBy"),
            "Region": region,
            "lambda_function_arns": lambda_function_arns,
            "sns_topic_arns": sns_topic_arns,
            "sqs_queue_arns": sqs_queue_arns,
            "ecs_cluster_arns": ecs_cluster_arns,
            "step_functions_arns": step_functions_arns,
            "kinesis_stream_arns": kinesis_stream_arns,
            "codebuild_project_arns": codebuild_project_arns,
            "codepipeline_arns": codepipeline_arns,
            "api_gateway_arns": api_gateway_arns,
            "cloudwatch_log_group_arns": cloudwatch_log_group_arns,
            "batch_job_queue_arns": batch_job_queue_arns,
            "sagemaker_pipeline_arns": sagemaker_pipeline_arns,
            "firehose_delivery_stream_arns": firehose_delivery_stream_arns,
            "redshift_cluster_arns": redshift_cluster_arns,
        }
        transformed.append(item)

    return transformed


def load_event_rules(
    neo4j_session: neo4j.Session,
    data: list[dict[str, Any]],
    region: str,
    aws_account_id: str,
    update_tag: int,
) -> None:
    """Ingest Event rules into the graph using the generic `load()` helper."""
    logger.info(
        "Loading %d CloudWatch Event rules for region %s into graph.",
        len(data),
        region,
    )
    # Track statistics for monitoring
    stat_handler.incr("eventbridge.rules.loaded", len(data))

    load(
        neo4j_session,
        EventRuleSchema(),
        data,
        lastupdated=update_tag,
        Region=region,
        AWS_ID=aws_account_id,
    )


def cleanup_event_rules(
    neo4j_session: neo4j.Session,
    common_job_parameters: dict[str, Any],
) -> None:
    logger.debug("Running CloudWatch Event rule cleanup job.")
    GraphJob.from_node_schema(EventRuleSchema(), common_job_parameters).run(
        neo4j_session
    )


@timeit
def sync(
    neo4j_session: neo4j.Session,
    boto3_session: boto3.session.Session,
    regions: list[str],
    current_aws_account_id: str,
    update_tag: int,
    common_job_parameters: dict[str, Any],
) -> None:
    """Entry-point called by the AWS ingestion pipeline."""

    for region in regions:
        logger.info(
            "Syncing CloudWatch Event rules for region %s in account %s.",
            region,
            current_aws_account_id,
        )
        raw_rules = get_event_rules(boto3_session, region)
        transformed_rules = transform_event_rules(raw_rules, region)
        load_event_rules(
            neo4j_session,
            transformed_rules,
            region,
            current_aws_account_id,
            update_tag,
        )

    cleanup_event_rules(neo4j_session, common_job_parameters)

    merge_module_sync_metadata(
        neo4j_session,
        group_type="AWSAccount",
        group_id=current_aws_account_id,
        synced_type="EventRule",
        update_tag=update_tag,
        stat_handler=stat_handler,
    )
