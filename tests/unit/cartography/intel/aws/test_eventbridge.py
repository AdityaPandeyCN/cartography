from cartography.intel.aws.eventbridge import transform_event_rules
from tests.data.aws.eventbridge.event_rules import MOCK_EVENT_RULES_RESPONSE

def test_transform_event_rules():
    """Validate the transform logic for EventBridge rules."""
    region = "us-east-1"

    result = transform_event_rules(MOCK_EVENT_RULES_RESPONSE, region)

    assert len(result) == 6

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
    assert ec2_rule["EventPattern"].startswith("{\"source\"")
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

