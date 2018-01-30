import boto3
import datetime
import json
import logging
import os
import sys
import time
import urllib2

MAX_BULK_SIZE_IN_BYTES = 1 * 1024 * 1024  # 1 MB

# Set logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# print correct response status code and return True if we need to retry
def shouldRetry(e):
    if e.code == 400:
        logger.error(
            "Got 400 code from Logz.io. This means that some of your logs are too big, or badly formatted. response: {0}".format(
                e.reason))
        return False
    elif e.code == 401:
        logger.error("You are not authorized with Logz.io! Token OK? dropping logs...")
        return False
    else:
        logger.error("Got {0} while sending logs to Logz.io, response: {1}".format(e.code, e.reason))
        return True


# send in bulk JSONs object to logzio
def sendToLogzio(jsonStrLogsList, logzioUrl):
    headers = {"Content-type": "application/json"}
    maxRetries = 3
    sleepBetweenRetries = 5
    for currTry in reversed(xrange(maxRetries)):
        request = urllib2.Request(logzioUrl, data='\n'.join(jsonStrLogsList), headers=headers)
        try:
            response = urllib2.urlopen(request)
            logger.info("Successfully sent bulk of " + str(len(jsonStrLogsList)) + " logs to Logz.io!")
            return
        except IOError as e:
            if shouldRetry(e):
                logger.info("Failure is retriable - Trying {} more times".format(currTry))
                time.sleep(sleepBetweenRetries)
            else:
                raise IOError("Failed to send logs")

    raise RuntimeError("Retries attempts exhausted. Failed sending to Logz.io")


# Set statistics parameters
def setMetricStats(metric, requestsMetaData):
    stats = {
        "Namespace": metric['Namespace'],
        "StartTime": requestsMetaData['startTime'],
        "EndTime": requestsMetaData['endTime'],
        "Period": requestsMetaData['Period'],
        "MetricName": metric['MetricName'],
        "Dimensions": metric['Dimensions']
    }

    if ('Statistics' in requestsMetaData) and (requestsMetaData['Statistics']):
        stats['Statistics'] = requestsMetaData['Statistics']
    elif ('ExtendedStatistics' in requestsMetaData) and (requestsMetaData['ExtendedStatistics']):
        stats['ExtendedStatistics'] = requestsMetaData['ExtendedStatistics']
    else:
        stats['Statistics'] = ['Average', 'Minimum', 'Maximum', 'SampleCount', 'Sum']

    return stats


# Enrich data point
def enrichDataPoint(dataPoint, metric):
    Timestamp = dataPoint.pop('Timestamp')
    timestamp = Timestamp.isoformat()
    dataPoint.update({"metric": metric['MetricName'], "@timestamp": timestamp, "Namespace": metric["Namespace"]})
    for dim in metric['Dimensions']:
        key = dim['Name']
        value = dim['Value']
        dataPoint[key] = value


# Going over all requests configurations to get statistics
def getMetricStatistics(client, statsRequestConfigurationsList):
    bulksOfStatisticsList = []
    statisticsList = []
    currentSize = 0
    for metric in statsRequestConfigurationsList:
        try:
            response = client.get_metric_statistics(**metric)
            if response["Datapoints"]:
                for dataPoint in response["Datapoints"]:
                    enrichDataPoint(dataPoint, metric)
                    jDataPoint = json.dumps(dataPoint)
                    statisticsList.append(jDataPoint)
                    currentSize += sys.getsizeof(jDataPoint)
                    if currentSize >= MAX_BULK_SIZE_IN_BYTES:
                        bulksOfStatisticsList.append(statisticsList)
                        currentSize = 0
                        statisticsList = []

        except Exception as e:
            logger.error("Exception from getMetricStatistics: {}".format(e))
            raise

    # add last bulk
    if statisticsList:
        bulksOfStatisticsList.append(statisticsList)

    return bulksOfStatisticsList


# Calling list_metrics with the correct configuration
def listMetric(conf, paginator):
    responseMetricsList = []
    for response in paginator.paginate(**conf):
        responseMetricsList += response['Metrics']

    return responseMetricsList


# Retrieve list metrics per configuration
def getListMetrics(cloudwatch, configurationFilePath, eventTime):
    responseMetricsList = []
    requestsMetaData = {}
    paginator = cloudwatch.get_paginator('list_metrics')
    with open(configurationFilePath, 'r') as f:
        jFile = json.load(f)
        for key in jFile:
            if key != 'Configurations':
                requestsMetaData[key] = jFile[key]

        requestsMetaData.update(getTimes(eventTime, jFile['TimeInterval']))

        for conf in jFile['Configurations']:
            responseMetricsList += listMetric(conf, paginator)

    logger.info("Received {} possible metric combinations".format(len(responseMetricsList)))
    return responseMetricsList, requestsMetaData


# Create all needed requests for statistics
def createStatsRequestList(metricsList, requestsMetaData):
    statsRequestConfigurationsList = []
    for metric in metricsList:
        if metric['Dimensions']:
            stats = setMetricStats(metric, requestsMetaData)
            statsRequestConfigurationsList.append(stats)

    return statsRequestConfigurationsList


def getTimes(eventTime, timeInterval):
    now = datetime.datetime.strptime(eventTime, '%Y-%m-%dT%H:%M:%SZ')
    startTime = (now - datetime.timedelta(minutes=timeInterval)).isoformat()
    endTime = now.isoformat()
    return {"startTime": startTime, "endTime": endTime}


def validateConfigurations():
    if (not "FILEPATH" in os.environ) or (not "TOKEN" in os.environ) or (not "URL" in os.environ):
        logger.error("Some environment variables are missing.")
        raise RuntimeError

    configurationFilePath = os.environ["FILEPATH"]
    try:
        with open(configurationFilePath, 'r') as f:
            try:
                jFile = json.load(f)
                timeInterval = jFile['TimeInterval']
                period = jFile['Period']
                configurations = jFile['Configurations']
            except (ValueError, KeyError) as e:
                logger.error("Error in your configuration file format: {}".format(e))
                raise
            else:
                if not isinstance(timeInterval, int):
                    logger.error("Error in your configuration file: TimeInterval should be int(min)")
                    raise RuntimeError

                if not isinstance(period, int):
                    logger.error("Error in your configuration file: Period should be int(sec)")
                    raise RuntimeError

                if (timeInterval * 60) < period:
                    logger.error("Error in your configuration file: TimeInterval can't be < period")
                    raise RuntimeError

                statistics = jFile.get("Statistics", [])
                optionalStatistics = ["Average", "Minimum", "Maximum", "SampleCount", "Sum"]
                for s in statistics:
                    if not s in optionalStatistics:
                        logger.error(
                            "Error in your configuration file: {0} is not part of {1}".format(s, optionalStatistics))
                        raise RuntimeError

                extendedStatistics = jFile.get("ExtendedStatistics", [])
                if statistics and extendedStatistics:
                    logger.error("Can't have both Statistics and ExtendedStatistics (according to boto3 documentation)")
                    raise RuntimeError

                for conf in configurations:
                    ns = conf.get("Namespace", [])
                    if not ns:
                        logger.error("Error in your configuration file: Namespace is a required field")
                        raise RuntimeError

                    try:
                        metricName = conf["MetricName"]
                        if not isinstance(metricName, str):
                            logger.error("Error in your configuration file: Period should be int(sec)")
                            raise RuntimeError
                    except KeyError:
                        # no MetricName is fine
                        pass

    except EnvironmentError as e:
        logger.error("You have an error in the configuration file path: {}".format(e))
        raise

    return timeInterval


def lambda_handler(event, context):
    validateConfigurations()
    # Use time from event
    try:
        eventTime = event['time']
    except KeyError:
        logger.error("No time field from the event")
        raise

    logger.info("Collecting Lambda metrics at {}".format(eventTime))
    logzioUrl = "{0}/?token={1}&type=cloudwatchmetrics".format(os.environ['URL'], os.environ['TOKEN'])
    configurationFilePath = os.environ['FILEPATH']
    cloudwatch = boto3.client("cloudwatch")

    metricsList, requestsMetaData = getListMetrics(cloudwatch, configurationFilePath, eventTime)
    statsRequestConfigurationsList = createStatsRequestList(metricsList, requestsMetaData)
    metricsJsonList = getMetricStatistics(cloudwatch, statsRequestConfigurationsList)

    for bulkJsonList in metricsJsonList:
        sendToLogzio(bulkJsonList, logzioUrl)
