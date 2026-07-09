# Real-Time Change Data Capture (CDC) Local Data Lakehouse

This project demonstrates a production-grade, local sandbox environment for real-time Change Data Capture (CDC) streaming database transactions into an analytics data lakehouse. It utilizes open-source software (OSS) without relying on abstract cloud services.

#### Architecture
![CDC Data Pipeline Architecture Diagram](./cdc-data-pipeline.png)

### Infrastructure Deployment & Component VerificationTo avoid network connection errors, services are brought up and validated component-by-component in chronological order.

### Step 1: Initialize and Verify PostgreSQLStart the transactional database engine to execute init.sql and provision the replication user:
```
docker compose up -d postgres
```

Verify the debezium service account role exists with Replication attributes and baseline seed data rows are present:
```
docker exec -it cdc_postgres psql -U app_user -d app_db -c "\du debezium"
docker exec -it cdc_postgres psql -U app_user -d app_db -c "SHOW wal_level; SELECT * FROM orders;"
```
*Do not proceed until wal_level outputs as logical and the role attributes match.*

### Step 2: Initialize and Verify Apache Kafka
Start the event streaming message bus broker running in KRaft mode:
```bash
docker compose up -d kafka
```
Verify the internal network broker API socket handles are fully open and responding:
```bash
docker exec -it cdc_kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

> ** Important Note: Why the Schema Registry is Not Used**
> In local sandboxes or personal development environments, a **Schema Registry container is entirely optional and has been intentionally omitted** from this pipeline to conserve system resources and save over 1.2 GB of disk/download space. 
> 
> * **Production Purpose:** In massive enterprise environments, a Schema Registry acts as a gatekeeper. It forces data into a compressed binary format (Avro) and prevents downstream analytics tools (like Spark) from crashing if a software engineer unexpectedly modifies database columns.
> * **Local Alternative:** Because you control both the database and the Spark script locally, there is no risk of unexpected schema changes breaking the pipeline. Instead, Debezium is configured to use native `JsonConverter` engines (`KEY_CONVERTER_SCHEMAS_ENABLE: false`), allowing plain text JSON to stream directly through Kafka topics to Spark completely registry-free.


### Step 3: Initialize Object Storage (MinIO)
Launch the lightweight local object storage container:
```
docker compose up -d minio
```

1. Open your web browser and navigate to the dashboard console at http://localhost:9001.
2. Authenticate using credentials: User: minio_admin / Password: minio_password.
3. Navigate to Buckets, click Create Bucket, and name it exactly: raw-zone


### Step 4: Initialize and Verify Debezium Connect
Start the distributed log parsing bridge application:
```
docker compose up -d debezium
```

Allow up to 15 seconds for the underlying Java Virtual Machine (JVM) configuration layers to attach, then check the web REST API health endpoint:
```
curl -s http://localhost:8083/
```
*A healthy container will immediately return an object declaring its running version.*

## 2. Connector Pipeline Registration & Event Bus Testing

### Step 5: Register the Connector Configuration Payload
Submit your configuration layout profile file directly to the active Debezium task registry manager:
```
curl -X POST -H "Content-Type: application/json" --data @config/pg-cdc-connectors.json http://localhost:8083/connectors
```

### Step 6: Verify the Live Connector Pipeline Task Status
Ensure that the top-level connector framework and the individual partition worker thread both report a status state of RUNNING:
```
curl -s http://localhost:8083/connectors/postgres-cdc-connector/status
```

### Step 7: Intercept the Baseline Kafka Event Stream
Open a new terminal window and attach a background console event listener queue reader to monitor your target Kafka topics:
```
docker exec -it cdc_kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic cdc.public.orders --from-beginning
```

## 3. Analytical Stream Processing & Lakehouse Validation
### Step 8: Initialize the Spark Processing GridLaunch your computational coordination master cluster nodes and engine execution instances:
```
docker compose up -d spark-master spark-worker
```

### Step 9: Deploy the PySpark Streaming Job
Submit your analytics transformation application tracking code pattern directly into the active master execution cluster environment:
```
docker exec -it cdc_spark_master /opt/spark/bin/spark-submit /opt/spark/apps/streaming_job.py
```
*This command line terminal thread will lock in place as a continuous foreground operation listening for structural changes.*

### Step 10: Inject Live Data Mutation & Verify Final Landing
Open another separate terminal window panel and issue a new row data mutation command into PostgreSQL to simulate application activity:
```
docker exec -it cdc_postgres psql -U app_user -d app_db -c "INSERT INTO public.orders (customer_name, amount, status) VALUES ('Oshoa Kimber', 150.70, 'PROCESSING');" 
```

Refresh your active browser panel tab inside your MinIO administrative dashboard at http://localhost:9001 inside the raw-zone bucket directory.You will see the fully structural checkpoints/ and delta/orders/ tracking folders dynamically generated containing valid, decoded, columnar Parquet lakehouse storage transactions.