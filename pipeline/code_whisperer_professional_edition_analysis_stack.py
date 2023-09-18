from aws_cdk import (
    Duration,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_s3 as s3,
    Stack
)
from pipeline.cloudtrail import CloudTrail
from pipeline.glue import Glue
from pipeline.kinesis_firehose import KinesisFirehose
from constructs import Construct

class CodeWhispererProfessionalEditionAnalysisStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        
        #Create a bucket for storing the access logs
        access_logs_bucket = s3.Bucket(
            self,
            "AccessLogsBucket",
            enforce_ssl=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(30),
                    noncurrent_version_expiration=Duration.days(30)
                )    
            ]
        )
        
        #Get the SSO Group IDs that were specified in a context argument
        group_ids_list = self.node.try_get_context("sso_group_ids")
        
        if group_ids_list is not None:
            group_ids = group_ids_list.split(',')
        #If no group ids were specified, set an empty List
        else:
            group_ids = []
        
        #Create the CloudTrail trail
        cloudtrail = CloudTrail(
            self,
            "CloudTrail",
            access_logs_bucket=access_logs_bucket
        )
        
        #Create a bucket for storing the processed events in
        codewhisperer_events_bucket = s3.Bucket(
            self,
            "CodeWhispererEventsBucket",
            enforce_ssl=True,
            server_access_logs_prefix="Logs/",
            server_access_logs_bucket=access_logs_bucket,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(120),
                    noncurrent_version_expiration=Duration.days(120)
                )    
            ]
        )
        
        #Create the accompanying Kinesis Data Firehose stream with all neccessary components
        firehose = KinesisFirehose(
            self,
            "KinesisFirehose",
            bucket=codewhisperer_events_bucket,
            group_ids=group_ids
        )
        
        #Create an EventBridge rule to trigger based on CodeWhisperer data event patterns
        rule = events.Rule(self, "CodeWhispererRule",
            event_pattern=events.EventPattern(
                source=['aws.codewhisperer'],
                detail_type=['AWS API Call via CloudTrail'],
                detail={
                    'eventSource': ['codewhisperer.amazonaws.com'],
                    'eventName': ['GenerateCompletions', 'GenerateRecommendations', 'ListCodeAnalysisFindings']
                }
            )
        )
        
        #Set the Firehose stream created as the target
        rule.add_target(events_targets.KinesisFirehoseStream(
            stream=firehose.stream
        ))
        
        #Create a Glue Data Crawler to populate our Data Catalog
        glue = Glue(
            self,
            "Glue",
            bucket=codewhisperer_events_bucket
        )
