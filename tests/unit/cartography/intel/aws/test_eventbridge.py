from cartography.intel.aws.eventbridge import classify_target_arn
from cartography.intel.aws.eventbridge import transform_event_rules
from tests.data.aws.eventbridge.event_rules import MOCK_EVENT_RULES_RESPONSE

def test_transform_event_rules():
    """Validate the transform logic for EventBridge rules."""
    region = "us-east-1"

    result = transform_event_rules(MOCK_EVENT_RULES_RESPONSE, region)

    assert len(result) == 7

    hourly_job = next(r for r in result if r["Name"] == "hourly-batch-job")
    assert hourly_job["ScheduleExpression"] == "rate(1 hour)"
    assert hourly_job["lambda_functions_arns"] == [
        "arn:aws:lambda:us-east-1:123456789012:function:ProcessBatchJob"
    ]
    assert hourly_job["sns_topics_arns"] == [
        "arn:aws:sns:us-east-1:123456789012:batch-notifications"
    ]

    ec2_rule = next(r for r in result if r["Name"] == "ec2-state-change")
    assert ec2_rule["EventPattern"].startswith('{"source"')
    assert ec2_rule["sqs_queues_arns"] == [
        "arn:aws:sqs:us-east-1:123456789012:ec2-events-queue"
    ]
    assert ec2_rule["kinesis_streams_arns"] == [
        "arn:aws:kinesis:us-east-1:123456789012:stream/event-stream"
    ]

    cross_rule = next(r for r in result if r["Name"] == "cross-account-events")
    assert cross_rule["ManagedBy"] == "partner-service"
    assert cross_rule["step_functions_arns"] == [
        "arn:aws:states:us-east-1:123456789012:stateMachine:ProcessCrossAccountEvents"
    ]
    assert cross_rule["ecs_clusters_arns"] == [
        "arn:aws:ecs:us-east-1:123456789012:cluster/processing-cluster"
    ]

    cb_rule = next(r for r in result if r["Name"] == "codebuild-trigger")
    assert cb_rule["codebuild_projects_arns"] == [
        "arn:aws:codebuild:us-east-1:123456789012:project/MyBuildProject"
    ]

    pipe_rule = next(r for r in result if r["Name"] == "pipeline-trigger")
    assert pipe_rule["ScheduleExpression"] == "cron(0 8 * * ? *)"
    assert pipe_rule["codepipelines_arns"] == [
        "arn:aws:codepipeline:us-east-1:123456789012:my-pipeline"
    ]

    api_rule = next(r for r in result if r["Name"] == "api-gateway-trigger")
    assert api_rule["api_gateways_arns"] == [
        "arn:aws:execute-api:us-east-1:123456789012:abcdef123/prod/POST/webhook"
    ]

    unknown_rule = next(r for r in result if r["Name"] == "unknown-target-test")
    assert unknown_rule["unknown_target_arns"] == [
        "arn:aws:some-future-service:us-east-1:123456789012:resource/unknown-type",
        "arn:aws:custom-service:us-east-1:123456789012:widget/my-widget",
    ]

def test_transform_event_rules_handles_missing_fields():
    """Ensure missing optional fields are handled gracefully."""
    minimal_data = {
        "Rules": [
            {
                "Name": "minimal-rule",
                "Arn": "arn:aws:events:us-east-1:123456789012:rule/minimal-rule",
            }
        ],
        "Targets": {},
    }

    result = transform_event_rules(minimal_data, "us-east-1")
    assert len(result) == 1
    rule = result[0]

    assert rule["Name"] == "minimal-rule"
    assert rule["EventBusName"] == "default"

def test_classify_target_arn():
    """Test the ARN classification function."""
    
    assert classify_target_arn("arn:aws:lambda:us-east-1:123:function:test") == (
        "lambda_functions",
        "arn:aws:lambda:us-east-1:123:function:test",
    )
    assert classify_target_arn("arn:aws:sns:us-east-1:123:topic") == (
        "sns_topics",
        "arn:aws:sns:us-east-1:123:topic",
    )
    assert classify_target_arn("arn:aws:sqs:us-east-1:123:queue") == (
        "sqs_queues",
        "arn:aws:sqs:us-east-1:123:queue",
    )
    assert classify_target_arn("arn:aws:ecs:us-east-1:123:cluster/test") == (
        "ecs_clusters",
        "arn:aws:ecs:us-east-1:123:cluster/test",
    )
    assert classify_target_arn("arn:aws:states:us-east-1:123:stateMachine:test") == (
        "step_functions",
        "arn:aws:states:us-east-1:123:stateMachine:test",
    )
    assert classify_target_arn("arn:aws:kinesis:us-east-1:123:stream/test") == (
        "kinesis_streams",
        "arn:aws:kinesis:us-east-1:123:stream/test",
    )
    assert classify_target_arn("arn:aws:codebuild:us-east-1:123:project/test") == (
        "codebuild_projects",
        "arn:aws:codebuild:us-east-1:123:project/test",
    )
    assert classify_target_arn("arn:aws:codepipeline:us-east-1:123:test") == (
        "codepipelines",
        "arn:aws:codepipeline:us-east-1:123:test",
    )
    assert classify_target_arn("arn:aws:execute-api:us-east-1:123:test") == (
        "api_gateways",
        "arn:aws:execute-api:us-east-1:123:test",
    )
    assert classify_target_arn("arn:aws:logs:us-east-1:123:log-group:test") == (
        "cloudwatch_log_groups",
        "arn:aws:logs:us-east-1:123:log-group:test",
    )

    assert classify_target_arn("arn:aws:unknown-service:us-east-1:123:resource") == (
        "unknown",
        "arn:aws:unknown-service:us-east-1:123:resource",
    )
    assert classify_target_arn("invalid-arn") == ("unknown", "invalid-arn")

