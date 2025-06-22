import boto3
import json
import gzip
from io import BytesIO
from flask import Flask, Response
from prometheus_client import generate_latest, Gauge

# AWS Configuration
REGION = "us-east-1"
BUCKET_NAME = "loki-logs-bpantala-975049893517-us-east-1"
PREFIX = "/"

# Initialize AWS Clients
s3_client = boto3.client("s3", region_name=REGION)

# Metrics Definitions
TOTAL_BYTES = Gauge("vpc_total_bytes", "Total bytes transferred in VPC Flow Logs")
REJECTED_PACKETS = Gauge("vpc_rejected_packets", "Total rejected packets in VPC Flow Logs")

# Flask App for HTTP Metrics Endpoint
app = Flask(__name__)

def fetch_flow_logs():
    """Fetch latest AWS VPC Flow Logs from S3 and update metrics."""
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    
    if "Contents" not in response or not response["Contents"]:
        return
    
    latest_object = max(response["Contents"], key=lambda obj: obj["LastModified"])
    s3_object = s3_client.get_object(Bucket=BUCKET_NAME, Key=latest_object["Key"])
    
    # Decompress the gzipped log file
    with gzip.GzipFile(fileobj=BytesIO(s3_object["Body"].read())) as gz:
        log_data = gz.read().decode("utf-8")
    
    total_bytes = 0
    rejected_packets = 0
    
    for line in log_data.splitlines():
        try:
            fields = line.split()
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
