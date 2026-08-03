package assay_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cuprite-io/assay"
)

func BenchmarkSampleSimple(b *testing.B) {
	backend := newMockBackend()
	sampler, err := assay.NewSampler(backend, assay.Config{MaxDepth: 32, MaxPaths: 1000})
	if err != nil {
		b.Fatalf("failed to create sampler: %v", err)
	}
	defer sampler.Close()

	payload := []byte(`{"id":123}`)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sampler.Sample(ctx, "bench-simple", payload)
	}
}

func BenchmarkSampleFlatMedium(b *testing.B) {
	backend := newMockBackend()
	sampler, err := assay.NewSampler(backend, assay.Config{MaxDepth: 32, MaxPaths: 1000})
	if err != nil {
		b.Fatalf("failed to create sampler: %v", err)
	}
	defer sampler.Close()

	payload := []byte(`{"id":123,"name":"Alice","active":true,"age":30,"email":"alice@example.com","score":99.5}`)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sampler.Sample(ctx, "bench-medium", payload)
	}
}

func BenchmarkSampleNestedDeep(b *testing.B) {
	backend := newMockBackend()
	sampler, err := assay.NewSampler(backend, assay.Config{MaxDepth: 32, MaxPaths: 1000})
	if err != nil {
		b.Fatalf("failed to create sampler: %v", err)
	}
	defer sampler.Close()

	payload := []byte(`{
		"user": {
			"id": "usr_100",
			"profile": {
				"name": {
					"first": "Alice",
					"last": "Smith"
				},
				"address": {
					"city": "San Francisco",
					"state": "CA",
					"zip": 94107,
					"coordinates": {
						"lat": 37.7749,
						"lng": -122.4194
					}
				},
				"tags": ["admin", "staff", "developer"]
			}
		}
	}`)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sampler.Sample(ctx, "bench-deep", payload)
	}
}

func BenchmarkSampleGoStructMedium(b *testing.B) {
	backend := newMockBackend()
	sampler, err := assay.NewSampler(backend, assay.Config{MaxDepth: 32, MaxPaths: 1000})
	if err != nil {
		b.Fatalf("failed to create sampler: %v", err)
	}
	defer sampler.Close()

	type Profile struct {
		Name  string `json:"name"`
		Age   int    `json:"age"`
		Email string `json:"email"`
	}
	type User struct {
		ID      int     `json:"id"`
		Active  bool    `json:"active"`
		Profile Profile `json:"profile"`
	}

	u := User{
		ID:     123,
		Active: true,
		Profile: Profile{
			Name:  "Alice",
			Age:   30,
			Email: "alice@example.com",
		},
	}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sampler.Sample(ctx, "bench-struct", u)
	}
}

func BenchmarkGetSchemaMedium(b *testing.B) {
	backend := newMockBackend()
	ctx := context.Background()
	schemaID := "bench-get-schema"
	for i := 0; i < 20; i++ {
		field := fmt.Sprintf("field_%d:string", i)
		_, _ = backend.MapIncrementBy(ctx, schemaID, field, 1.0)
	}

	sampler, err := assay.NewSampler(backend, assay.Config{})
	if err != nil {
		b.Fatalf("failed to create sampler: %v", err)
	}
	defer sampler.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sampler.GetSchema(ctx, schemaID)
	}
}
