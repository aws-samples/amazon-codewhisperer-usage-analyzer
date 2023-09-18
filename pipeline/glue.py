from constructs import Construct
from aws_cdk import (
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
    Stack as stack
)
from cdk_nag import NagSuppressions
from cdk_nag import NagPackSuppression
import os

class Glue(Construct):
    
    def __init__(
        self,
        scope: Construct,
        id_: str,
        bucket: s3.IBucket
    ):
        super().__init__(scope, id_)
        
        #Create the Glue IAM Role and set relevant permissions
        crawler_role = iam.Role(self, "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com')
        )
        
        crawler_role.add_to_policy(
            iam.PolicyStatement(
                resources=["*"],
                actions=[
                    "glue:BatchCreatePartition",
                    "glue:BatchGetPartition",
                    "glue:CreateTable",
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "logs:PutLogEvents"
                ]
            )
        )
        
        #Allow Glue to interact with the CodeWhisperer events bucket
        bucket.grant_read(crawler_role)
        
        NagSuppressions.add_resource_suppressions(
            crawler_role,
            [NagPackSuppression(id="AwsSolutions-IAM5", reason="IAM permissions restricted to this specific bucket, used for crawling the bucket")],
            True
        )
        
        #Create a Glue database which will be able to be queried in Amazon Athena
        database = glue.CfnDatabase(self, "CodeWhispererEventsDatabase",
            catalog_id=stack.of(self).account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="codewhisperer_events"
            )
        )
        
        #Create a Crawler to run once every 6 hours (UTC) to create a new partition for the day
        crawler = glue.CfnCrawler(self, "CodeWhispererEventsCrawler",
            role=crawler_role.role_arn,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    #Do not scan the Errors folder
                    exclusions=["Errors/**"],
                    path="{}/CodeWhispererEvents/".format(bucket.bucket_name),
                    sample_size=5
                )]
            ),
            database_name="codewhisperer_events",
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression="cron(0 */6 * * ? *)"
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_NEW_FOLDERS_ONLY"
            )
        )
        
        glue.CfnDataCatalogEncryptionSettings(self, "MyCfnDataCatalogEncryptionSettings",
            catalog_id=database.catalog_id,
            data_catalog_encryption_settings=glue.CfnDataCatalogEncryptionSettings.DataCatalogEncryptionSettingsProperty(
                encryption_at_rest=glue.CfnDataCatalogEncryptionSettings.EncryptionAtRestProperty(
                    catalog_encryption_mode="SSE-KMS"
                )
            )
        )