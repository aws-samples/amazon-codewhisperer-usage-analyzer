import os
import base64
import boto3
import json

user_details = {}
group_details = {}
client = boto3.client('identitystore')

def lambda_handler(event, context):
    
    #Create the records list to replace the records entry after processing
    records = []
    
    #Loop through each record
    for record in event['records']:
        
        #Data is included in base64 format, decode it and load the JSON string to use the object
        record_data_raw = json.loads(base64.b64decode(record['data']))
        
        #Only retrieve the events where an SSO user identity is included, this will remove calls from supported service integrations like Lambda. If nextToken is included and has a value we should not include it as the result was not returned
        if 'onBehalfOf' in record_data_raw["detail"]["userIdentity"] and ("nextToken" not in record_data_raw["detail"]["requestParameters"] or record_data_raw["detail"]["requestParameters"]["nextToken"] == ""):
            #Assume the event is a code suggestion...
            event_type = "CodeSuggestionInvocation"
            
            #Unless it is a ListCodeAnalysisFindings event
            if record_data_raw["detail"]["eventName"] == "ListCodeAnalysisFindings":
                event_type = "SecurityScanInvocation"
        
            #Create the new processed format by extracting key details from the event
            record_data = {
                "event_time": record_data_raw["detail"]["eventTime"],
                "account_id": record_data_raw["detail"]["userIdentity"]["accountId"],
                "user_id": record_data_raw["detail"]["userIdentity"]["onBehalfOf"]["userId"],
                "identity_store_arn": record_data_raw["detail"]["userIdentity"]["onBehalfOf"]["identityStoreArn"],
                "event_type": event_type
            }
            
            #if FileContext is included also capture the programming language that was used
            if "fileContext" in record_data_raw["detail"]["requestParameters"]:
                record_data["programming_language"] = record_data_raw["detail"]["requestParameters"]["fileContext"]["programmingLanguage"]["languageName"]
                
            #If SSO_GROUP_IDS is included then gather additional metadata and enrich the above record
            if os.environ["SSO_GROUP_IDS"] != "":
                record_data = append_sso_details(record_data, os.environ["SSO_GROUP_IDS"])
            
            #Convert the entity back into a JSON string and append a newline character to avoid grouped results residing in the same line
            record_data = json.dumps(record_data)
            record_data += "\n"
    
            #Base64 encode the result and let Firehose know it was processed correctly
            record['data'] = base64.b64encode(record_data.encode('utf-8'))
            record['result'] = 'Ok'
        #This record should not be put into our S3 bucket, mark it as dropped so that Firehose will not reattempt to process
        else:
            record['result'] = 'Dropped'
            
        #Add to the records List
        records.append(record)
    
    #Return newly formatted events to be published into their destination
    event['records'] = records
    
    return event

#This function takes a individual records data and attempts to add additional data via the IAM Identity Center groups
def append_sso_details(record_data, sso_group_ids):
    
    #Extract the idenitity store id from the Arn
    identity_store_id = record_data['identity_store_arn'].split("/")[1]
    
    #If we alrady have cached the user_name use that rather than performing the API call
    if record_data['user_id'] in user_details:
        record_data['user_name'] = user_details[record_data['user_id']]['user_name']
    else:
        #Loop us the user details
        user_details_result = client.describe_user(
            IdentityStoreId=identity_store_id,
            UserId=record_data['user_id']
        )
        
        #Append user_name to the record data
        record_data['user_name'] = user_details_result['UserName']
        
        #Cache the result for later invocations
        user_details[record_data['user_id']] = {
            'user_name': record_data['user_name']
        }
    
    #Convert the SSO group IDs inot a list by splitting on the , character
    sso_group_ids = sso_group_ids.split(',')
    
    #If we already have a group_id set in the cached details reuse it. We can safely assume that if this logic has been carried out the group will also be cached
    if 'group_id' in user_details[record_data['user_id']]:
        record_data['group_id'] = user_details[record_data['user_id']]['group_id']
        record_data['group_name'] = group_details[record_data['group_id']]
    #The users groups are not known so look up if they are part of at least one of the groups
    else:
        is_member_in_groups = client.is_member_in_groups(
            IdentityStoreId=identity_store_id,
            MemberId={
                'UserId': record_data['user_id']
            },
            GroupIds=sso_group_ids
        )
        
        #Loop over the results
        for result in is_member_in_groups['Results']:
            #If the user if a part of the group proceeed with the logic
            if result['MembershipExists'] == True:
                #Amend the record_data and cache the group_id for later
                user_details[record_data['user_id']]['group_id'] = result['GroupId']
                record_data['group_id'] = result['GroupId']
                
                #If the group has been looked up before and is cached, reuse the details
                if result['GroupId'] in group_details:
                    record_data['group_name'] = result['GroupId']
                #Otherwise do a describe_group call to get the group name
                else:
                    group_description = client.describe_group(
                        IdentityStoreId=identity_store_id,
                        GroupId=record_data['group_id']
                    )
                    
                    #Amend the record_data and store the group name in the cache
                    record_data['group_name'] = group_description['DisplayName']
                    group_details[record_data['group_id']] = group_description['DisplayName']
                
                #Break out of the loop by returning the record data
                return record_data
    
    return record_data