from constructs import Construct
from aws_cdk import (
    aws_cloudtrail as cloudtrail,
    aws_iam as iam,
    aws_s3 as s3,
    Duration,
    Stack as stack
)

class CloudTrail(Construct):
    
    def __init__(
        self,
        scope: Construct,
        id_: str,
        access_logs_bucket: s3.IBucket
    ):
        super().__init__(scope, id_)
        
        cloudTrailPrincipal = iam.ServicePrincipal('cloudtrail.amazonaws.com')

        #A trail requires bucket create one to store these events in
        self.trail_s3 = s3.Bucket(
            self,
            'CodeWhispererCloudTrailBucket',
            enforce_ssl=True,
            server_access_logs_prefix="Logs/",
            server_access_logs_bucket=access_logs_bucket,
            versioned=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(7),
                    noncurrent_version_expiration=Duration.days(7)
                )    
            ]
        )

        #Create a CloudTrail in CDK which will only have data events
        self.codewhisperer_trail = cloudtrail.CfnTrail(
            self,
            'CodeWhispererCloudTrail',
            is_logging=True,
            s3_bucket_name=self.trail_s3.bucket_name,
            enable_log_file_validation=True,
            is_multi_region_trail=True,
            include_global_service_events=True,
            
            #Advanced selector to capture CodeWhisperer Data events only
            advanced_event_selectors=[cloudtrail.CfnTrail.AdvancedEventSelectorProperty(
                field_selectors=[cloudtrail.CfnTrail.AdvancedFieldSelectorProperty(
                    field='eventCategory',
                    equal_to=['Data']
                ),
                cloudtrail.CfnTrail.AdvancedFieldSelectorProperty(
                    field='resources.type',
                    equal_to=['AWS::CodeWhisperer::Profile']
                )]
            )]
        )

        #Set dependency between CloudTrail and S3 bucket policy
        self.codewhisperer_trail.node.add_dependency(self.trail_s3.policy)
        
        #Set permissions for CloudTrail to interact with the S3 bucket
        self.trail_s3.add_to_resource_policy(
            iam.PolicyStatement(
                resources=[self.trail_s3.bucket_arn],
                actions=['s3:GetBucketAcl'],
                principals=[cloudTrailPrincipal],
            ),
        )
        
        self.trail_s3.add_to_resource_policy(
            iam.PolicyStatement(
                resources=[self.trail_s3.arn_for_objects('AWSLogs/{}/*'.format(stack.of(self).account))],
                actions=['s3:PutObject'],
                principals=[cloudTrailPrincipal],
                conditions={
                    'StringEquals': {
                        's3:x-amz-acl': 'bucket-owner-full-control',
                    }
                }
            )
        )