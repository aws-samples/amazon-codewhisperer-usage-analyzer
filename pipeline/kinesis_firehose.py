from constructs import Construct
from aws_cdk import (
    aws_kinesisfirehose as kinesisfirehose,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    Duration as duration,
    Stack as stack
)
from cdk_nag import NagSuppressions
from cdk_nag import NagPackSuppression
import os
from pathlib import Path

class KinesisFirehose(Construct):
    
    def __init__(
        self,
        scope: Construct,
        id_: str,
        bucket: s3.IBucket,
        group_ids: [],
        sso_region: str
    ):
        super().__init__(scope, id_)
        
        #Create a role for Firehose and greant permissions
        firehose_role = iam.Role(self, "KinesisFirehoseRole",
            assumed_by=iam.ServicePrincipal('firehose.amazonaws.com')
        )
        
        bucket.grant_read_write(firehose_role, "*")
        
        environment_variables = {
            'SSO_GROUP_IDS': ','.join(group_ids)
        }
        
        if sso_region != None:
            environment_variables['SSO_REGION'] = sso_region
        
        #Create the Lambda function that will be used for processing
        transformer_function = lambda_.Function(self, "FirehoseTransformationLambda",
            code=lambda_.Code.from_asset(os.path.join(Path.cwd(), "pipeline", "firehose_transformation")),
            handler="index.lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            environment=environment_variables,
            #This should be more than enough for the majority of usecases. Results are cached in the function so lookups and processing should be fast
            memory_size=512,
            timeout=duration.seconds(60),
            tracing=lambda_.Tracing.ACTIVE
        )
        
        #Allow the function to be invoked by Firehose
        transformer_function.grant_invoke(firehose_role)
        
        NagSuppressions.add_resource_suppressions(
            transformer_function,
            [NagPackSuppression(id="AwsSolutions-IAM4", reason="Default policy contains required permissions for Lambda to function such as writing logs")],
            True
        )

        NagSuppressions.add_resource_suppressions(
            transformer_function,
            [NagPackSuppression(id="AwsSolutions-IAM5", reason="IAM permissions managed by L2 Lambda construct")],
            True
        )

        NagSuppressions.add_resource_suppressions(
            firehose_role,
            [NagPackSuppression(id="AwsSolutions-IAM5", reason="IAM permissions restricted to this specific bucket, used for pushing events into")],
            True
        )

        
        #Check if SSO Group IDs were passed in context. If they were then add permissions to read the user details and groups.
        if len(group_ids) > 0:
            transformer_function.add_to_role_policy(
                    iam.PolicyStatement(
                    resources=[
                        "*"
                    ],
                    actions=[
                        "identitystore:DescribeUser",
				        "identitystore:IsMemberInGroups"
                    ]
                )
            )
            
            group_resources = [
                "arn:aws:identitystore::{}:identitystore/*".format(stack.of(self).account)
            ]
            
            for group_id in group_ids:
                group_resources.append("arn:aws:identitystore:::group/{}".format(group_id))
            
            transformer_function.add_to_role_policy(
                    iam.PolicyStatement(
                    resources=group_resources,
                    actions=[
				        "identitystore:DescribeGroup"
                    ]
                )
            )
            
            NagSuppressions.add_resource_suppressions(
                transformer_function,
                [NagPackSuppression(id="AwsSolutions-IAM5", reason="Wildcards are unavoidable as it needs to cover all users and groups to extract information")],
                True
            )
        
        #Create a Kinesis Data Firehose stream that will publish to an S3 bucket
        self.stream = kinesisfirehose.CfnDeliveryStream(self, "KinesisFirehoseStream",
            delivery_stream_type='DirectPut',
            delivery_stream_encryption_configuration_input = kinesisfirehose.CfnDeliveryStream.DeliveryStreamEncryptionConfigurationInputProperty(
                key_type="AWS_OWNED_CMK",
            ),
            extended_s3_destination_configuration=kinesisfirehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=bucket.bucket_arn,
                role_arn=firehose_role.role_arn,
                #Buffer every 5 minutes to avoid creating large volumes of small files
                buffering_hints=kinesisfirehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=300,
                    size_in_m_bs=10
                ),
                compression_format="GZIP",
                #Prefix the processed events and errors using Year, Month and Day. This allows partitioning to maximise Athena performance
                error_output_prefix="Errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/",
                prefix="CodeWhispererEvents/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/",
                #Include a Lambda processor to remove unneeded data and enrich with SSO data (where groups IDs were supplied)
                processing_configuration=kinesisfirehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                    enabled=True,
                    processors=[kinesisfirehose.CfnDeliveryStream.ProcessorProperty(
                        type="Lambda",
                        parameters=[kinesisfirehose.CfnDeliveryStream.ProcessorParameterProperty(
                            parameter_name="LambdaArn",
                            parameter_value=transformer_function.function_arn
                        )]
                    )]
                )
            )
        )