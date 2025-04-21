"""Mock JSON-RPC server for demonstration purposes.

This is a simple JSON-RPC server that implements the methods needed
by the sample tap. It serves as both an example and a test fixture.

To run the server:
    python mock_jsonrpc_server.py

The server will listen on http://localhost:8000 by default.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Sample data - Note: using random here is OK for demo purposes
# In a real app, consider using secrets module for anything security-related
ITEMS = [
    {
        "id": i,
        "name": f"Item {i}",
        "description": f"This is sample item {i}",
        "category": ["Electronics", "Books", "Clothing", "Food"][
            i % 4
        ],  # Deterministic choice
        "price": (i * 100) % 10000 + 100,  # Deterministic price
        "created_at": (
            datetime.now(timezone.utc) - timedelta(days=i % 100 + 1)
        ).isoformat(),
    }
    for i in range(1, 101)  # Generate 100 items
]

SERVICE_INFO = {
    "id": "sample-jsonrpc-service",
    "name": "Sample JSON-RPC Service",
    "version": "1.0.0",
    "features": ["info", "items", "pagination"],
}


class JSONRPCHandler(BaseHTTPRequestHandler):
    """HTTP request handler for JSON-RPC requests."""

    def post(self):
        """Handle POST requests with JSON-RPC payloads."""
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length)

        try:
            request = json.loads(post_data.decode("utf-8"))

            # Validate JSON-RPC request
            if "jsonrpc" not in request or request["jsonrpc"] != "2.0":
                self._send_error(
                    -32600, "Invalid Request: Not a valid JSON-RPC 2.0 request"
                )
                return

            if "method" not in request:
                self._send_error(-32600, "Invalid Request: Method not specified")
                return

            if "id" not in request:
                self._send_error(-32600, "Invalid Request: ID not specified")
                return

            # Process the method
            method = request["method"]
            params = request.get("params", {})
            request_id = request["id"]

            # Handle supported methods
            if method == "get_service_info":
                self._send_result(request_id, SERVICE_INFO)
            elif method == "get_items":
                self._handle_get_items(request_id, params)
            else:
                self._send_error(-32601, f"Method not found: {method}")

        except json.JSONDecodeError:
            self._send_error(-32700, "Parse error: Invalid JSON")
        except ValueError as e:
            self._send_error(-32602, f"Invalid params: {e}")
        except Exception:
            logger.exception("Internal server error")
            self._send_error(-32603, "Internal error")

    # Alias for http.server compatibility
    # Using a property to avoid N815 violation while maintaining compatibility
    @property
    def do_POST(self):  # noqa: N802
        """Handle POST requests. Required by http.server."""
        return self.post

    def _send_result(self, request_id, result):
        """Send a successful JSON-RPC response.

        Args:
            request_id: The request ID from the client.
            result: The result to return.
        """
        response = {"jsonrpc": "2.0", "result": result, "id": request_id}
        self._send_response(response)

    def _send_error(self, code, message):
        """Send an error JSON-RPC response.

        Args:
            code: The error code.
            message: The error message.
        """
        response = {
            "jsonrpc": "2.0",
            "error": {"code": code, "message": message},
            "id": None,
        }
        self._send_response(response)

    def _send_response(self, response_obj):
        """Send a JSON-RPC response to the client.

        Args:
            response_obj: The response object to send.
        """
        response_json = json.dumps(response_obj)

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.send_header("Content-length", len(response_json))
        self.end_headers()

        self.wfile.write(response_json.encode("utf-8"))

    def _handle_get_items(self, request_id, params):
        """Handle the get_items method with pagination.

        Args:
            request_id: The request ID from the client.
            params: The request parameters.
        """
        page = params.get("page", 1)
        limit = params.get("limit", 10)

        # Calculate pagination
        start_index = (page - 1) * limit
        end_index = start_index + limit

        # Get page of items
        items_page = ITEMS[start_index:end_index]

        # Calculate total pages
        total_pages = (len(ITEMS) + limit - 1) // limit

        # Return paginated result
        result = {
            "items": items_page,
            "page": page,
            "limit": limit,
            "total_items": len(ITEMS),
            "total_pages": total_pages,
        }

        self._send_result(request_id, result)


def run_server(host="localhost", port=8000):
    """Run the mock JSON-RPC server.

    Args:
        host: The hostname to listen on.
        port: The port to listen on.
    """
    server_address = (host, port)
    httpd = HTTPServer(server_address, JSONRPCHandler)
    logger.info("Starting mock JSON-RPC server on http://%s:%s", host, port)
    logger.info("Press Ctrl+C to stop the server")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.server_close()
        logger.info("Server stopped")


if __name__ == "__main__":
    run_server()
