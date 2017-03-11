package server

import "context"

type contextKey string

var registryClientKey contextKey = "registryClient"

// RegistryClientFrom returns the registry client stored in ctx, if present. It will panic otherwise.
func RegistryClientFrom(ctx context.Context) RegistryClient {
	return ctx.Value(registryClientKey).(RegistryClient)
}

// WithRegistryClient creates a new context with provided registry client.
func WithRegistryClient(ctx context.Context, client RegistryClient) context.Context {
	return context.WithValue(ctx, registryClientKey, client)
}
