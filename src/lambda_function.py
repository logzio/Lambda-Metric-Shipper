import boto3
import datetime
import json
import logging
import os

from shipper import LogzioShipper

# Set logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Set statistics parameters
def _set_metric_stats(metric, requests_meta_data):
    # type: (dict, dict) -> dict
    stats = {
        "Namespace": metric['Namespace'],
        "StartTime": requests_meta_data['startTime'],
        "EndTime": requests_meta_data['endTime'],
        "Period": requests_meta_data['Period'],
        "MetricName": metric['MetricName'],
        "Dimensions": metric['Dimensions']
    }

    if ('Statistics' in requests_meta_data) and (requests_meta_data['Statistics']):
        stats['Statistics'] = requests_meta_data['Statistics']
    elif ('ExtendedStatistics' in requests_meta_data) and (requests_meta_data['ExtendedStatistics']):
        stats['ExtendedStatistics'] = requests_meta_data['ExtendedStatistics']
    else:
        stats['Statistics'] = ['Average', 'Minimum', 'Maximum', 'SampleCount', 'Sum']

    return stats


# Enrich data point
def _enrich_data_point(data_point, metric):
    # type: (dict, dict) -> None
    timestamp = data_point.pop('Timestamp')
    ts = timestamp.isoformat()
    data_point.update({"metric": metric['MetricName'], "@timestamp": ts, "Namespace": metric["Namespace"]})
    for dim in metric['Dimensions']:
        key = dim['Name']
        value = dim['Value']
        data_point[key] = value


# Going over all requests configurations to get statistics
def _get_metric_statistics(cloudwatch, stats_request_configurations_list, logzio_url):
    # type: ('boto3.client("cloudwatch")', list, str) -> None
    shipper = LogzioShipper(logzio_url)
    for metric in stats_request_configurations_list:
        try:
            response = cloudwatch.get_metric_statistics(**metric)
            if response["Datapoints"]:
                for dp in response["Datapoints"]:
                    _enrich_data_point(dp, metric)
                    shipper.add(dp)
        except Exception as e:
            logger.error("Exception from getMetricStatistics: {}".format(e))
            raise

    shipper.flush()


# Calling list_metrics with the correct configuration
def _list_metric(conf, paginator):
    # type: (dict, 'botocore.client.CloudWatch.Paginator.ListMetrics') -> list
    response_metrics_list = []
    for response in paginator.paginate(**conf):
        response_metrics_list += response['Metrics']

    return response_metrics_list


# Retrieve list metrics per configuration
def _get_list_metrics(cloudwatch, fp, event_time):
    # type: ('boto3.client("cloudwatch")', str, str) -> (list, dict)
    response_metrics_list = []
    requests_meta_data = {}
    paginator = cloudwatch.get_paginator('list_metrics')
    with open(fp, 'r') as f:
        jfile = json.load(f)
        for key in jfile:
            if key != 'Configurations':
                requests_meta_data[key] = jfile[key]

                requests_meta_data.update(_get_times(event_time, jfile['TimeInterval']))

        for conf in jfile['Configurations']:
            response_metrics_list += _list_metric(conf, paginator)

    logger.info("Received {} possible metric combinations".format(len(response_metrics_list)))
    return response_metrics_list, requests_meta_data


# Create all needed requests for statistics
def _create_stats_request_list(metrics_list, requests_meta_data):
    # type: (list, dict) -> list
    stats_request_configurations_list = []
    for metric in metrics_list:
        if metric['Dimensions']:
            stats = _set_metric_stats(metric, requests_meta_data)
            stats_request_configurations_list.append(stats)

    return stats_request_configurations_list


def _get_times(event_time, time_interval):
    # type: (str, int) -> dict
    now = datetime.datetime.strptime(event_time, '%Y-%m-%dT%H:%M:%SZ')
    start_time = (now - datetime.timedelta(minutes=time_interval)).isoformat()
    end_time = now.isoformat()
    return {"startTime": start_time, "endTime": end_time}


def validate_configurations():
    # type: (None) -> None
    if ("FILEPATH" not in os.environ) or ("TOKEN" not in os.environ) or ("URL" not in os.environ):
        logger.error("Some environment variables are missing.")
        raise RuntimeError

    configuration_fp = os.environ["FILEPATH"]
    try:
        with open(configuration_fp, 'r') as f:
            try:
                jfile = json.load(f)
                time_interval = jfile['TimeInterval']
                period = jfile['Period']
                configurations = jfile['Configurations']
            except (ValueError, KeyError) as e:
                logger.error("Error in your configuration file format: {}".format(e))
                raise
            else:
                if not isinstance(time_interval, int):
                    logger.error("Error in your configuration file: TimeInterval should be int(min)")
                    raise RuntimeError

                if not isinstance(period, int):
                    logger.error("Error in your configuration file: Period should be int(sec)")
                    raise RuntimeError

                if (time_interval * 60) < period:
                    logger.error("Error in your configuration file: TimeInterval can't be < period")
                    raise RuntimeError

                statistics = jfile.get("Statistics", [])
                optional_statistics = ["Average", "Minimum", "Maximum", "SampleCount", "Sum"]
                for s in statistics:
                    if s not in optional_statistics:
                        logger.error(
                            "Error in your configuration file: {0} is not part of {1}".format(s, optional_statistics))
                        raise RuntimeError

                extended_statistics = jfile.get("ExtendedStatistics", [])
                if statistics and extended_statistics:
                    logger.error("Can't have both Statistics and ExtendedStatistics (according to boto3 documentation)")
                    raise RuntimeError

                for conf in configurations:
                    ns = conf.get("Namespace", [])
                    if not ns:
                        logger.error("Error in your configuration file: Namespace is a required field")
                        raise RuntimeError

                    try:
                        metric_name = conf["MetricName"]
                        if not isinstance(metric_name, str):
                            logger.error("Error in your configuration file: Period should be int(sec)")
                            raise RuntimeError
                    except KeyError:
                        # no MetricName is fine
                        pass

    except EnvironmentError as e:
        logger.error("You have an error in the configuration file path: {}".format(e))
        raise


def lambda_handler(event, context):
    # type: (dict, 'LambdaContext') -> None
    validate_configurations()
    # Use time from event
    try:
        event_time = event['time']
    except KeyError:
        logger.error("No time field from the event")
        raise

    logger.info("Collecting Lambda metrics at {}".format(event_time))
    logzio_url = "{0}/?token={1}&type=cloudwatchmetrics".format(os.environ['URL'], os.environ['TOKEN'])
    configuration_fp = os.environ['FILEPATH']
    cloudwatch = boto3.client("cloudwatch")

    metrics_list, requests_meta_data = _get_list_metrics(cloudwatch, configuration_fp, event_time)
    stats_request_configurations_list = _create_stats_request_list(metrics_list, requests_meta_data)
    _get_metric_statistics(cloudwatch, stats_request_configurations_list, logzio_url)
