# Webhook Listener Service

A FastAPI service for receiving and processing webhook payloads with direct Delta Lake writes for real-time ACID transactions.

## Features

- Receives POST requests at `/webhooks` endpoint
- Direct writes to Delta Lake using native Rust engine
- Real-time processing with 1-2 second latency
- Date-based partitioning for optimal query performance
- Health check endpoint at `/health`
- Automatic API documentation at `/docs`

## API Endpoints

- `GET /` - Service info (includes Delta Lake status)
- `GET /health` - Health check
- `POST /webhooks` - Receive webhook payload (writes directly to Delta Lake)
- `GET /webhooks/count` - Get Delta Lake processing status
- `GET /webhooks/status` - Get webhook service status
- `GET /docs` - Swagger UI documentation

## Delta Lake Storage

Webhooks are written directly to Delta Lake table at:
`s3://webhook-notifications/bronze/webhooks/`

Each record contains:
- `webhook_id`: Unique identifier
- `webhook_timestamp`: ISO format timestamp
- `raw_payload`: Complete webhook JSON payload
- `transaction_signature`: Extracted transaction signature
- `account_address`: Extracted account address
- `token_address`: Extracted token address (if applicable)
- `webhook_type`: Type of webhook event
- `processing_date`: Date partition for efficient querying
- `processed_to_silver`: Processing status flag

## Architecture

```
Helius Webhooks → FastAPI (port 8000) → Native Delta Lake (Bronze) 
    → Airflow DAG (DuckDB) → Delta Lake (Silver)
```

## Performance

- **Latency**: 1-2 seconds per webhook
- **Throughput**: 1000+ webhooks/minute
- **Storage**: ACID transactions with Delta Lake
- **Technology**: Native Delta Lake with Rust engine (no JVM overhead)

## Environment Variables

- `DATA_DIR`: Directory for temporary files (default: `/app/data`)
- `AWS_ENDPOINT_URL`: MinIO endpoint (default: `http://minio:9000`)
- `AWS_ACCESS_KEY_ID`: MinIO access key (default: `minioadmin`)
- `AWS_SECRET_ACCESS_KEY`: MinIO secret key (default: `minioadmin123`)

## Development

```bash
cd services/webhook-listener
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

## Docker

```bash
docker build -t webhook-listener .
docker run -p 8000:8000 webhook-listener
```

## Testing

```bash
# Test webhook endpoint
curl -X POST http://localhost:8000/webhooks \
  -H "Content-Type: application/json" \
  -d '{"test": "webhook", "signature": "test123"}'

# Check health
curl http://localhost:8000/health

# View API docs
open http://localhost:8000/docs
```