# CloudWatch Metrics Shipper - Monitoring Lambda

This is an AWS Lambda function that scheduled to collect CloudWatch metrics and send them to Logz.io in bulk, over HTTP, .

## Step 1 - Creating the Lambda Function

1. Sign in to your AWS account and open the AWS Lambda console.
2. Click **Create function**, to create a new Lambda function.
3. Select Author from scratch, and enter the following information:
  - Name -  Enter a name for your new Lambda function. We suggest adding the log type to the name.
  - Runtime - From the drop-down menu, select Python 2.7 as the function’s runtime.
  - Role - Make sure to add the following policy to your Lambda role:
  
  
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Stmt1338559372809",
                "Action": [
                    "cloudwatch:GetMetricStatistics",
                    "cloudwatch:ListMetrics",
                    "cloudwatch:DescribeAlarms"
                ],
                "Effect": "Allow",
                "Resource": "*"
            }
        ]
    }   

4. Hit the **Create Function** button in the bottom-right corner of the page.

## Step 2 - Uploading and configuring the Logz.io Lambda shipper

1. In the Function Code section, open the Code entry type menu, and select *Edit code inline*.
2. Copy the Lambda function in this repository into the editor.
3. In the Environment variables section, set your Logz.io token, URL and log type:
    - TOKEN: Your Logz.io account token. Can be retrieved on the Settings page in the Logz.io UI.
    - FILEPATH: Relative file path to the configuration JSON file.
    - URL: the Logz.io listener URL. If you are in the EU region insert https://listener-eu.logz.io:8071. Otherwise, use https://listener.logz.io:8071. You can tell which region you are in by checking your login URL - *app.logz.io* means you are in the US. *app-eu.logz.io* means you are in the EU.

4. In the Basic Settings section, we recommend to start by setting memory to 512(MB) and a 3(MIN) timeout, and then subsequently adjusting these values based on trial and error, and according to your Lambda usage.
5. Leave the other settings as default
6. Create a configuration JSON file. To make it easier to understand how to configure the JSON configuration file for our Lambda, we decided to comply with the “list_metrics” function from boto3 documentation, in addition to a few more parameters we allow to configure.


    {
        "TimeInterval": int,
	    "Period": int,
	    "Statistics": ["Average", "Minimum", "Maximum", "SampleCount", "Sum"],
		   "ExtendedStatistics": ["string",],
	    "Configurations": [{
		        "Namespace": "string",
		        "MetricName": "string",
		        "Dimensions": [{
			            "Name": "string",
			            "Value": "string"
		        }]
	    }]

    }

Parameters:
- TimeInterval **[REQUIRED]** - The time period to monitor, in minutes, before the Lambda was invoked. Set to the same value as the schedule event time interval.
- Period **[REQUIRED]** - The granularity, in seconds, of the returned data points. For metrics with regular resolution, a period can be as short as one minute (60 seconds) and must be a multiple of 60.
- Statistics - The metric statistics.
- ExtendedStatistics - The percentile statistics. Specify values between p0.0 and p100. You can have Statistics or ExtendedStatistics in your configuration file, but **not** both.
- Configurations **[REQUIRED]** - A list of JSONs. Each of them consists of “Namespace” key and an optional “Dimensions” and “MetricName” keys.
   
## Step 3 - Setting CloudWatch log event trigger
1. Under Add triggers at the top of the page, select the CloudWatch event trigger.
2. In the Configure triggers section, select 'Create a new rule' and enter the 'Rule Name' and 'Rule Description'. 
3. Under 'Rule type' select 'Schedule expression', and in the 'Schedule expression' tab enter your 'TimeInterval' as your rate. For example, 'rate(5 minutes)' 
4. Click **Add** to add the trigger and **Save** at the top of the page to save all your configurations.

[here]: https://support.logz.io/hc/en-us/articles/210205985-Which-log-types-are-preconfigured-on-the-Logz-io-platform-
