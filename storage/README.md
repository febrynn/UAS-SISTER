# Storage Service Documentation

The storage service is responsible for persisting event data in a reliable and efficient manner. It utilizes a relational database to ensure data integrity and support for complex queries.

## Database Schema

The storage service uses the following tables:

1. **processed_events**: Stores unique events that have been processed.
   - Columns: `topic`, `event_id`, `timestamp`, `source`, `payload`

2. **outbox**: Temporarily holds events that need to be published to the aggregator.
   - Columns: `topic`, `event_id`, `timestamp`, `source`, `payload`, `processed`

3. **stats**: Maintains statistics about event processing.
   - Columns: `received`, `unique_processed`, `duplicate_dropped`, `topics`, `uptime`

## Initialization

The database schema is defined in the `init.sql` file. This file should be executed to set up the necessary tables before the application starts.

## Usage

The storage service is integrated with the aggregator service, which interacts with it to store and retrieve event data. Ensure that the database connection details are correctly configured in the environment variables.

## Persistence

Data is persisted using PostgreSQL, ensuring durability and consistency. The service is designed to handle concurrent access and maintain data integrity through transactions.

## Health Checks

Implement health checks to monitor the status of the storage service and ensure it is operational.

## Additional Information

Refer to the main project README for overall architecture and service interactions.