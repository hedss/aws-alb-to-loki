/*
    This is an example app to retrieve AWS ALB logs and then send them to a Loki endpoint.
    This is *no way* production ready, but serves as an example.
    LOKI_USER is your Grafana Cloud Logs User ID.
    LOKI_PASSWORD is the metrics writing token for the Grafana Cloud Logs User.
    LOKI_ENDPOINT is the full endpoint to the Loki instance.

    This uses AWS SNS to retrieve events when a new ALB logfile is stored to S3. It then
    retrieves this logfile from S3, decompresses it and send the raw log data to Loki.
    Additional work would be to JSONify these logs with appropriate key/value pairs first
    to make LogQL queries easier (and not require regexp user for capture patterns).

    Another way to do this would be as a Lambda, where you essentially do the same
    work, retrieving SNS notfications, retrieving from the S3 bucket and then directly
    sending to Loki.

    Steps to get ALB logs into Loki:
    1. Create a new S3 bucket to store your ALB logs.
    1. Ensure you enable ALB logs on your ALB. These will go to the S3 bucket you created in the prior step:
        EC2 -> Load balancers -> Select ALB and choose 'Edit attributes' -> Select 'Access logs' and choose S3 location
    2. Create a new SNS topic and ensure there is a suitable endpoint. In this example, an HTTP endpoint:
        Simple Notification Service -> Create topic -> Give it a name -> Save changes
        Be aware you must use the subscription link sent to your listening endpoint first to subscribe to future events!
        https://docs.aws.amazon.com/sns/latest/dg/sns-create-subscribe-endpoint-to-topic.html
    3. Ensure that the SNS topic has a ARN role suitable for S3 to use:
        {
          "Version": "2012-10-17",
          "Id": "albid",
          "Statement": [
            {
              "Sid": "alb-bucket-statement",
              "Effect": "Allow",
              "Principal": {
                "Service": "s3.amazonaws.com"
              },
              "Action": "SNS:Publish",
              "Resource": "<topicARN>",
              "Condition": {
                "StringEquals": {
                  "aws:SourceAccount": "<ownerSourceAccount>"
                },
                "ArnLike": {
                  "aws:SourceArn": "arn:aws:s3:*:*:<bucket>"
                }
              }
            }
          ]
        }
    4. Create event notifications on S3 for the topic you created:
        S3->Select S3 Bucket for ALB logs->Event notifications->Create notification
        Give it a name, select 'All object create events' for Event types, select
        'SNS topic' and then select the SNS topic you created.

    After these steps, when a new ALB log is stored in the S3 bucket, the SNS
    topic will send a notification to the HTTP endpoint with the relevant
    details.
*/
// Grab NPM requirements
const AWS = require('aws-sdk');
const Express = require('express');
const parser = require('body-parser');
const request = require('request-promise');
const { ungzip } = require('node-gzip');

// Create reqs
const s3 = new AWS.S3();
const snsListener = Express();
snsListener.use(parser.text());

// Ensure we only send logs after the last time we parsed them
let lastUpdated = new Date();

// Get envvars
const LOKI_USER=process.env.LOKI_USER;
const LOKI_PASSWORD=process.env.LOKI_PASSWORD;
const LOKI_ENDPOINT=process.env.LOKI_ENDPOINT;
logs-prod-us-central1.grafana.net/loki/api/v1/push

// Fetch logs from S3, turn them to raw strings.
async function fetchLogObject(details) {
    const params = {
            Bucket: details.bucket.name,
            Key: details.logObject.key
        };

    try {
        // Get log object
        const logObject = await s3.getObject(params).promise();
        // GUnzip to raw log entries.
        const data = await ungzip(logObject.Body);
        return data.toString();
    } catch (err) {
        console.log(err);
    }

    return null;
}

// Send logs to Loki.
async function sendToLoki(logData) {
    const logArray = logData.split('\n');
    logArray.pop();
    const re = /^(?<type>.*?)\s+(?<time>.*?)\s+(?<elb>.*?)\s+(?<client>.*?)\s+(?<target>.*?)\s+(?<req_proc_time>.*?)\s+(?<target_proc_time>.*?)\s+(?<resp_proc_time>.*?)\s+(?<elb_status>.*?)\s+(?<target_status>.*?)\s+(?<recv_bytes>.*?)\s+(?<sent_bytes>.*?)\s+\"(?<request>.*?)"\s+"(?<user_agent>.*?)"\s+-\s+-\s+(?<target_group_arn>.*?)\s+"(?<trace_id>.*?)"\s+.*?(?<match_priority>[0-9]+)\s+(?<req_creation_time>.*?)\s+"(?<action_executed>.*?)"\s+.*?"-"\s+"-"\s+\"(?<target_list>.*?)"\s+"(?<status_code>.*?)"\s+/;
    for (let index = 0; index < logArray.length; index++) {
        const line = logArray[index];
        const groups = line.match(re);
        let jsonLogObj = {};
        let keyIndex = 1;

        // Format into JSON object
        [ 'type', 'time', 'elb', 'client', 'target', 'req_proc_time', 'target_proc_time', 'resp_proc_time',
            'elb_status', 'target_status', 'recv_bytes', 'sent_bytes', 'request', 'user_agent', 'target_group_arn', 'trace_id',
            'match_priority', 'req_creation_time', 'action_executed', 'target_list', 'status_code'].forEach(key => {
            jsonLogObj[key] = groups[keyIndex++];
        });
        const jsonLogLine = JSON.stringify(jsonLogObj);

        // Logging system sends to Loki
        // Uses a Grafana Cloud Logs endpoint.
        // Only one process is pulling ALB logs, but if we had two or more
        // we'd ID each individual one by changing the 'job' tag so we don't
        // get OoO timestamps as they'll amount to different streams.
        // Have a read of https://grafana.com/blog/2020/09/16/how-were-making-it-easier-to-use-the-loki-logging-system-with-aws-lambda-and-other-short-lived-services/
        // for a good overview of this.
        try {
            await request(
                {
                    uri: `https://${LOKI_USER}:${LOKI_PASSWORD}@${LOKI_ENDPOINT}`,
                    method: 'POST',
                    json: true,
                    body: {
                        'streams': [
                            {
                                'stream': {
                                    // These are the labels to attach to the log entries
                                    'job': 'alb-logger-1',
                                    'level': 'INFO',
                                },
                                'values': [
                                    // Timestamps are in nanoseconds (ms * 1000000)
                                    [`${Date.now() * 1000000}`, jsonLogLine]
                                ]
                            }
                        ]
                    }
                },
            );
        } catch (err) {
            console.log(`Logging error: ${err}`);
        }
    }
}

// We only act on getting a request from the SNS notification
snsListener.post('/', async (req, res) => {
    const newBody = JSON.parse(req.body);
    const snsMessage = JSON.parse(newBody.Message);
    // On a creation event, we get the file, read the data and then send
    // directly to Loki
    snsMessage.Records.forEach(async (record) => {
        const eventTime = new Date(record.eventTime);
        if (eventTime.getTime() > lastUpdated.getTime()) {
            const s3 = record.s3;
            if (record.eventName === 'ObjectCreated:Put') {
                const logData = await fetchLogObject({
                    bucket: s3.bucket,
                    logObject: s3.object
                });

                if (!logData) {
                    console.log(`Couldn't get log data for ${snsMessage.bucket.name}/${snsMessage.object.Key}`);
                } else {
                    await sendToLoki(logData);
                }
            }
            lastUpdated = eventTime;
        }
    });

    // All is well. Probably.
    res.sendStatus(200);
});

snsListener.listen('3456', () => {
    console.log('Listening for SNS notifications');
});
