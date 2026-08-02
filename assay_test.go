package assay_test

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cuprite-io/assay"
)

type mockBackend struct {
	mu    sync.Mutex
	stats map[string]map[string]float64
}

func newMockBackend() *mockBackend {
	return &mockBackend{
		stats: make(map[string]map[string]float64),
	}
}

func (m *mockBackend) MapIncrementBy(ctx context.Context, key, field string, delta float64) (float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fields, ok := m.stats[key]
	if !ok {
		fields = make(map[string]float64)
		m.stats[key] = fields
	}
	fields[field] += delta
	return fields[field], nil
}

func (m *mockBackend) MapGetAll(ctx context.Context, key string) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fields, ok := m.stats[key]
	if !ok {
		return make(map[string]string), nil
	}

	res := make(map[string]string)
	for k, v := range fields {
		res[k] = strconv.FormatFloat(v, 'f', -1, 64)
	}
	return res, nil
}

func TestSampleCorrectness(t *testing.T) {
	backend := newMockBackend()
	sampler := assay.NewSampler(backend, assay.Config{
		MaxDepth:      10,
		MaxPaths:      100,
		FlushInterval: 10 * time.Millisecond,
	})
	defer sampler.Close()

	ctx := context.Background()
	schemaID := "test-schema"

	// Sample 1: A flat JSON object
	payload1 := []byte(`{"id": 123, "name": "Alice", "active": true}`)
	err := sampler.Sample(ctx, schemaID, payload1)
	if err != nil {
		t.Fatalf("failed to sample: %v", err)
	}

	node, err := sampler.GetSchema(ctx, schemaID)
	if err != nil {
		t.Fatalf("failed to get schema: %v", err)
	}

	if node.Type != "object" {
		t.Errorf("expected root type 'object', got %q", node.Type)
	}

	// Verify path values in returned schema
	children := node.Children
	if children == nil {
		t.Fatal("expected children nodes, got nil")
	}

	idNode, exists := children["id"]
	if !exists {
		t.Error("expected 'id' node to exist")
	} else {
		if idNode.Type != "number" {
			t.Errorf("expected 'id' type 'number', got %q", idNode.Type)
		}
		if !idNode.Required {
			t.Error("expected 'id' to be required")
		}
	}

	nameNode, exists := children["name"]
	if !exists {
		t.Error("expected 'name' node to exist")
	} else {
		if nameNode.Type != "string" {
			t.Errorf("expected 'name' type 'string', got %q", nameNode.Type)
		}
	}
}

func TestOptionalityAndProbability(t *testing.T) {
	backend := newMockBackend()
	sampler := assay.NewSampler(backend, assay.Config{
		MaxDepth:      10,
		MaxPaths:      100,
		FlushInterval: 10 * time.Millisecond,
	})
	defer sampler.Close()

	ctx := context.Background()
	schemaID := "opt-prob-schema"

	// Sample 2 payloads:
	// Payload 1 has optional field "email"
	// Payload 2 does not have "email", and "age" is a string instead of number
	p1 := []byte(`{"id": 1, "email": "alice@gmail.com", "age": 25}`)
	p2 := []byte(`{"id": 2, "age": "thirty"}`)

	err := sampler.Sample(ctx, schemaID, p1)
	if err != nil {
		t.Fatalf("failed sample 1: %v", err)
	}

	err = sampler.Sample(ctx, schemaID, p2)
	if err != nil {
		t.Fatalf("failed sample 2: %v", err)
	}

	node, err := sampler.GetSchema(ctx, schemaID)
	if err != nil {
		t.Fatalf("failed to get schema: %v", err)
	}

	children := node.Children
	if children == nil {
		t.Fatal("expected children nodes")
	}

	// 'id' should be present in 2/2 samples and never null -> required
	idNode := children["id"]
	if !idNode.Required {
		t.Error("expected 'id' to be required")
	}

	// 'email' should be present in 1/2 samples -> optional
	emailNode, exists := children["email"]
	if !exists {
		t.Error("expected 'email' to exist in schema")
	} else {
		if emailNode.Required {
			t.Error("expected 'email' to be optional (Required=false)")
		}
	}

	// 'age' is a number in p1 and string in p2 -> mixed type, both 50% probability
	ageNode := children["age"]
	if ageNode.Type != "mixed" {
		t.Errorf("expected 'age' type to be 'mixed', got %q", ageNode.Type)
	}
	probStr := ageNode.Probability["string"]
	probNum := ageNode.Probability["number"]
	if probStr != 0.5 || probNum != 0.5 {
		t.Errorf("expected 'age' type probabilities to be 0.5, got string=%f, number=%f", probStr, probNum)
	}
}

func TestNestedDataAndArrays(t *testing.T) {
	backend := newMockBackend()
	sampler := assay.NewSampler(backend, assay.Config{
		MaxDepth:      10,
		MaxPaths:      100,
		FlushInterval: 10 * time.Millisecond,
	})
	defer sampler.Close()

	ctx := context.Background()
	schemaID := "nested-schema"

	p := []byte(`{
		"user": {
			"profile": {
				"tags": ["admin", "user"]
			}
		}
	}`)

	err := sampler.Sample(ctx, schemaID, p)
	if err != nil {
		t.Fatalf("failed to sample nested structure: %v", err)
	}

	node, err := sampler.GetSchema(ctx, schemaID)
	if err != nil {
		t.Fatalf("failed to get schema: %v", err)
	}

	// Navigate: user -> profile -> tags -> *
	userNode := node.Children["user"]
	if userNode == nil || userNode.Type != "object" {
		t.Fatalf("expected 'user' object node, got %v", userNode)
	}

	profileNode := userNode.Children["profile"]
	if profileNode == nil || profileNode.Type != "object" {
		t.Fatalf("expected 'profile' object node, got %v", profileNode)
	}

	tagsNode := profileNode.Children["tags"]
	if tagsNode == nil || tagsNode.Type != "array" {
		t.Fatalf("expected 'tags' array node, got %v", tagsNode)
	}

	wildcardNode := tagsNode.Children["*"]
	if wildcardNode == nil || wildcardNode.Type != "string" {
		t.Fatalf("expected 'tags.*' string node, got %v", wildcardNode)
	}
}

func TestGoStructsAndMaps(t *testing.T) {
	backend := newMockBackend()
	sampler := assay.NewSampler(backend, assay.Config{
		MaxDepth:      10,
		MaxPaths:      100,
		FlushInterval: 10 * time.Millisecond,
	})
	defer sampler.Close()

	type Profile struct {
		Age  int    `json:"age"`
		City string `json:"location_city"`
	}

	type User struct {
		ID      string  `json:"id"`
		Active  bool    `json:"active"`
		Profile Profile `json:"profile"`
	}

	u := User{
		ID:     "usr_1",
		Active: true,
		Profile: Profile{
			Age:  28,
			City: "San Francisco",
		},
	}

	ctx := context.Background()
	err := sampler.Sample(ctx, "struct-schema", u)
	if err != nil {
		t.Fatalf("failed to sample Go struct: %v", err)
	}

	node, err := sampler.GetSchema(ctx, "struct-schema")
	if err != nil {
		t.Fatalf("failed to get schema: %v", err)
	}

	// Verify structure match
	idNode := node.Children["id"]
	if idNode == nil || idNode.Type != "string" {
		t.Errorf("expected 'id' node of type string")
	}

	profileNode := node.Children["profile"]
	if profileNode == nil || profileNode.Type != "object" {
		t.Fatalf("expected 'profile' node of type object")
	}

	cityNode := profileNode.Children["location_city"]
	if cityNode == nil || cityNode.Type != "string" {
		t.Errorf("expected 'location_city' node of type string, got %v", cityNode)
	}
}

func TestMaxDepthAndMaxPathsLimits(t *testing.T) {
	backend := newMockBackend()

	t.Run("Max Depth", func(t *testing.T) {
		sampler := assay.NewSampler(backend, assay.Config{MaxDepth: 2})
		defer sampler.Close()

		// Deep nesting (3 levels)
		p := []byte(`{"a": {"b": {"c": 1}}}`)
		err := sampler.Sample(context.Background(), "limit-schema-1", p)
		if err != assay.ErrMaxDepthExceeded {
			t.Errorf("expected ErrMaxDepthExceeded, got %v", err)
		}
	})

	t.Run("Max Paths", func(t *testing.T) {
		sampler := assay.NewSampler(backend, assay.Config{MaxPaths: 3})
		defer sampler.Close()

		// We have root "", "a", "b", "c", which is 4 paths. MaxPaths is 3.
		// "a", "b", "c" should fill it up and drop anything past 3 paths.
		p := []byte(`{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5}`)
		err := sampler.Sample(context.Background(), "limit-schema-2", p)
		if err != nil {
			t.Fatalf("failed to sample: %v", err)
		}

		node, err := sampler.GetSchema(context.Background(), "limit-schema-2")
		if err != nil {
			t.Fatalf("failed to get schema: %v", err)
		}

		// Check keys at top level (including root, total unique paths in stats is capped)
		if len(node.Children) > 3 {
			t.Errorf("expected children count to be capped at 2 (since root is 1 path), got %d: %v", len(node.Children), node.Children)
		}
	})
}

func TestLocalAggregationAndFlush(t *testing.T) {
	backend := newMockBackend()
	sampler := assay.NewSampler(backend, assay.Config{
		MaxDepth:      10,
		MaxPaths:      100,
		FlushInterval: 50 * time.Millisecond,
	})
	defer sampler.Close()

	ctx := context.Background()
	schemaID := "flush-schema"

	// Sample 5 times
	p := []byte(`{"id": 1}`)
	for i := 0; i < 5; i++ {
		err := sampler.Sample(ctx, schemaID, p)
		if err != nil {
			t.Fatalf("failed to sample: %v", err)
		}
	}

	// Initially, let's look at mockBackend. FetchStats directly from mockBackend.
	// Since the background flusher has not completed (or runs every 50ms),
	// wait a bit for flush.
	time.Sleep(100 * time.Millisecond)

	backend.mu.Lock()
	fields, ok := backend.stats[schemaID]
	var val float64
	if ok {
		val = fields["id:number"]
	}
	backend.mu.Unlock()

	if val != 5.0 {
		t.Fatalf("expected 5 observed count in backend, got %f", val)
	}
}

func TestTreeReconstructionCorrectness(t *testing.T) {
	// Let's test buildSchemaTree directly with custom stats map
	stats := map[string]*assay.PathStatsSnapshot{
		"": {
			ObservedCount: 10,
			TypeCounts:    [6]uint64{0, 0, 0, 0, 10, 0}, // Root object (TypeObject is 4)
		},
		"user": {
			ObservedCount: 10,
			TypeCounts:    [6]uint64{0, 0, 0, 0, 10, 0},
		},
		"user.name": {
			ObservedCount: 10,
			TypeCounts:    [6]uint64{0, 10, 0, 0, 0, 0}, // string (TypeString is 1)
		},
		"user.age": {
			ObservedCount: 8,
			TypeCounts:    [6]uint64{0, 0, 8, 0, 0, 0}, // number (TypeNumber is 2)
		},
		"user.email": {
			ObservedCount: 5,
			TypeCounts:    [6]uint64{1, 4, 0, 0, 0, 0}, // null=1, string=4
		},
	}

	root := assay.ExportBuildSchemaTree(stats, 10)

	user := root.Children["user"]
	if user == nil {
		t.Fatal("expected 'user'")
	}
	if !user.Required {
		t.Error("expected 'user' to be required since observed 10/10 times")
	}

	name := user.Children["name"]
	if name == nil {
		t.Fatal("expected 'user.name'")
	}
	if !name.Required {
		t.Error("expected 'user.name' to be required since 10/10 and null count is 0")
	}

	age := user.Children["age"]
	if age == nil {
		t.Fatal("expected 'user.age'")
	}
	if age.Required {
		t.Error("expected 'user.age' to be optional (observed 8/10)")
	}

	email := user.Children["email"]
	if email == nil {
		t.Fatal("expected 'user.email'")
	}
	if email.Required {
		t.Error("expected 'user.email' to be optional (observed 5/10)")
	}
	if email.Type != "mixed" {
		t.Errorf("expected type 'mixed', got %q", email.Type)
	}
	if email.Probability["null"] != 0.2 || email.Probability["string"] != 0.8 {
		t.Errorf("incorrect probabilities: %v", email.Probability)
	}
}

// Convert struct helper
func marshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
