from cartography.intel.aws.eventbridge import classify_target_arn
from cartography.intel.aws.eventbridge import transform_event_rules
from tests.data.aws.eventbridge.event_rules import MOCK_EVENT_RULES_RESPONSE


def test_transform_event_rules():
    """Validate the transform logic for EventBridge rules."""
    region = "us-east-1"

    result = transform_event_rules(MOCK_EVENT_RULES_RESPONSE, region)

    assert len(result) == 7

    # Validate schedule rule targets
    hourly_job = next(r for r in result if r["Name"] == "hourly-batch-job")
    assert hourly_job["ScheduleExpression"] == "rate(1 hour)"
    assert hourly_job["lambda_function_arns"] == [
        "arn:aws:lambda:us-east-1:123456789012:function:ProcessBatchJob"
    ]
    assert hourly_job["sns_topic_arns"] == [
        "arn:aws:sns:us-east-1:123456789012:batch-notifications"
    ]

    # Validate event rule with mixed targets
    ec2_rule = next(r for r in result if r["Name"] == "ec2-state-change")
    assert ec2_rule["EventPattern"].startswith('{"source"')
    assert ec2_rule["sqs_queue_arns"] == [
        "arn:aws:sqs:us-east-1:123456789012:ec2-events-queue"
    ]
    assert ec2_rule["kinesis_stream_arns"] == [
        "arn:aws:kinesis:us-east-1:123456789012:stream/event-stream"
    ]

    # Cross-account rule
    cross_rule = next(r for r in result if r["Name"] == "cross-account-events")
    assert cross_rule["ManagedBy"] == "partner-service"
    assert cross_rule["step_functions_arns"] == [
        "arn:aws:states:us-east-1:123456789012:stateMachine:ProcessCrossAccountEvents"
    ]
    assert cross_rule["ecs_cluster_arns"] == [
        "arn:aws:ecs:us-east-1:123456789012:cluster/processing-cluster"
    ]

    # CodeBuild rule
    cb_rule = next(r for r in result if r["Name"] == "codebuild-trigger")
    assert cb_rule["codebuild_project_arns"] == [
        "arn:aws:codebuild:us-east-1:123456789012:project/MyBuildProject"
    ]

    # Pipeline rule
    pipe_rule = next(r for r in result if r["Name"] == "pipeline-trigger")
    assert pipe_rule["ScheduleExpression"] == "cron(0 8 * * ? *)"
    assert pipe_rule["codepipeline_arns"] == [
        "arn:aws:codepipeline:us-east-1:123456789012:my-pipeline"
    ]

    # API Gateway rule
    api_rule = next(r for r in result if r["Name"] == "api-gateway-trigger")
    assert api_rule["api_gateway_arns"] == [
        "arn:aws:execute-api:us-east-1:123456789012:abcdef123/prod/POST/webhook"
    ]

    # Unknown target rule
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
    assert rule["lambda_function_arns"] == []


def test_classify_target_arn():
    """Test the ARN classification function."""
    # Test known target types
    assert classify_target_arn("arn:aws:lambda:us-east-1:123:function:test") == (
        "lambda_function",
        "arn:aws:lambda:us-east-1:123:function:test",
    )
    assert classify_target_arn("arn:aws:sns:us-east-1:123:topic") == (
        "sns_topic",
        "arn:aws:sns:us-east-1:123:topic",
    )
    assert classify_target_arn("arn:aws:sqs:us-east-1:123:queue") == (
        "sqs_queue",
        "arn:aws:sqs:us-east-1:123:queue",
    )
    assert classify_target_arn("arn:aws:ecs:us-east-1:123:cluster/test") == (
        "ecs_cluster",
        "arn:aws:ecs:us-east-1:123:cluster/test",
    )
    assert classify_target_arn("arn:aws:states:us-east-1:123:stateMachine:test") == (
        "step_function",
        "arn:aws:states:us-east-1:123:stateMachine:test",
    )
    assert classify_target_arn("arn:aws:kinesis:us-east-1:123:stream/test") == (
        "kinesis_stream",
        "arn:aws:kinesis:us-east-1:123:stream/test",
    )
    assert classify_target_arn("arn:aws:codebuild:us-east-1:123:project/test") == (
        "codebuild_project",
        "arn:aws:codebuild:us-east-1:123:project/test",
    )
    assert classify_target_arn("arn:aws:codepipeline:us-east-1:123:test") == (
        "codepipeline",
        "arn:aws:codepipeline:us-east-1:123:test",
    )
    assert classify_target_arn("arn:aws:execute-api:us-east-1:123:test") == (
        "api_gateway",
        "arn:aws:execute-api:us-east-1:123:test",
    )
    assert classify_target_arn("arn:aws:logs:us-east-1:123:log-group:test") == (
        "cloudwatch_log_group",
        "arn:aws:logs:us-east-1:123:log-group:test",
    )

    # Test unknown target type
    assert classify_target_arn("arn:aws:unknown-service:us-east-1:123:resource") == (
        "unknown",
        "arn:aws:unknown-service:us-east-1:123:resource",
    )
    assert classify_target_arn("invalid-arn") == ("unknown", "invalid-arn")
