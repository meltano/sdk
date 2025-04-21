# Sample JSON-RPC Tap

This is a sample [Singer](https://www.singer.io/) tap that demonstrates how to use the Singer SDK to extract data from a JSON-RPC API endpoint.

## Overview

This sample tap demonstrates:

1. How to use the `JSONRPCStream` base class
2. How to handle JSON-RPC 2.0 request/response structures
3. How to implement pagination with JSON-RPC APIs
4. How to validate and handle JSON-RPC error responses
5. How to map JSON-RPC parameters to stream state

The tap includes a mock JSON-RPC server for demonstration and testing purposes.

## Usage

### Step 1: Start the Mock Server

Start the mock JSON-RPC server in one terminal:

```bash
python mock_jsonrpc_server.py
```

The server will listen on http://localhost:8000.

### Step 2: Run the Tap

Run the tap in another terminal:

```bash
python -m sample_tap_jsonrpc --config config.json
```

## Configuration

Configuration options:

```json
{
  "endpoint_url": "http://localhost:8000",
  "batch_size": 10
}
```

- `endpoint_url` (required): The URL for the JSON-RPC API endpoint
- `batch_size` (optional): Number of items to request in each batch (default: 10)

## Streams

This tap provides the following streams:

### 1. service_info

Basic information about the JSON-RPC service.

### 2. items

A paginated collection of items from the service.

## Implementation Details

### JSON-RPC Request Format

This tap follows the JSON-RPC 2.0 specification, sending requests in this format:

```json
{
  "jsonrpc": "2.0",
  "method": "METHOD_NAME",
  "params": { ... },
  "id": "UNIQUE_ID"
}
```

### Response Handling

The tap expects responses in this format:

```json
{
  "jsonrpc": "2.0",
  "result": { ... },
  "id": "UNIQUE_ID"
}
```

Or error responses:

```json
{
  "jsonrpc": "2.0",
  "error": {
    "code": -32000,
    "message": "Error message"
  },
  "id": "UNIQUE_ID"
}
```

### Pagination

The tap implements pagination by sending page numbers in the request parameters and parsing pagination metadata from the response.
