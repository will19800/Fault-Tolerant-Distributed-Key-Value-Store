// Package api exposes key-value operations to HTTP clients.
//
// The single-node API has four routes:
//
//	PUT    /v1/keys/{key}  stores the raw request body and returns 204
//	GET    /v1/keys/{key}  returns raw value bytes or 404
//	DELETE /v1/keys/{key}  removes the key and returns 204
//	GET    /healthz        returns {"status":"ok"}
//
// Values use application/octet-stream so clients can store text, JSON, or
// arbitrary binary data. Errors use application/json with code and message
// fields. Keys are limited to MaxKeyBytes, and PUT bodies are limited by the
// value configured when the handler is constructed.
package api
