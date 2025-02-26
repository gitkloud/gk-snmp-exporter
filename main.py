import boto3
import json
import time
from flask import Flask, Response
from prometheus_client import generate_latest, Gauge
from pysnmp.entity import engine, config
from pysnmp.entity.rfc3413 import cmdrsp

# AWS Configuration
REGION = "us-east-1"
LOG_GROUP = "/gitkloud/default/vpc-flow-logs"

# Initialize AWS Clients
logs_client = boto3.client("logs", region_name="us-east-1")

# Metrics Definitions
TOTAL_BYTES = Gauge("vpc_total_bytes", "Total bytes transferred in VPC Flow Logs")
REJECTED_PACKETS = Gauge("vpc_rejected_packets", "Total rejected packets in VPC Flow Logs")

# Flask App for HTTP Metrics Endpoint
app = Flask(__name__)

def fetch_flow_logs():
    """Fetch latest AWS VPC Flow Logs and update metrics."""
    response = logs_client.describe_log_streams(
        logGroupName=LOG_GROUP, orderBy="LastEventTime", descending=True, limit=1
    )
    
    if "logStreams" not in response or not response["logStreams"]:
        return
    
    log_stream_name = response["logStreams"][0]["logStreamName"]
    log_events = logs_client.get_log_events(
        logGroupName=LOG_GROUP, logStreamName=log_stream_name, limit=50
    )
    
    total_bytes = 0
    rejected_packets = 0
    
    for event in log_events["events"]:
        try:
            fields = event["message"].split()
            total_bytes += int(fields[9])  # bytes transferred
            if fields[11] == "REJECT":
                rejected_packets += 1
        except (IndexError, ValueError):
            continue
    
    # Update Prometheus Metrics
    TOTAL_BYTES.set(total_bytes)
    REJECTED_PACKETS.set(rejected_packets)

@app.route("/metrics")
def metrics():
    fetch_flow_logs()
    return Response(generate_latest(), mimetype="text/plain")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
