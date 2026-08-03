# Assay: High-Performance Data Sampling & Schema Inference Engine

<p align="center">
  <img src="assets/mascot.png" alt="Assay Mascot" width="200" height="200" />
</p>

`assay` is a lightweight, ultra-fast Go library designed to profile incoming data streams in real-time, infer their schema, and track properties like type probabilities and field requirement (optionality) on-the-fly.

Engineered for hot-path execution in high-throughput data planes, `assay` operates with sub-millisecond overhead and is capable of processing high-volume data streams with sub-microsecond ingestion latency.

---

## ✨ Key Features
* **Dynamic Schema Inference:** Infers root-level and deeply nested object/array schemas on raw JSON bytes or native Go values (structs, maps).
* **Type Probability Tracking:** Computes type distribution percentage per key path (e.g., how often `user.age` is a `string` vs. a `number`).
* **Optionality Detection:** Evaluates field requirement recursively relative to parent objects based on observed frequencies.
* **Storage Decoupling:** Fully storage-agnostic. Define a simple interface to persist statistics to a local sharded in-memory cache, Redis, or [Capacitor](file:///home/shantanu/Projects/cuprite-io/capacitor).
* **High Performance:** Implements zero-allocation path traversal, local write-back aggregation buffers, and streaming JSON token parsing to minimize GC pressure.
* **Safety First:** Prevents stack overflows and Out-Of-Memory (OOM) situations via maximum depth limits and path cardinality caps.

---

## 📦 Installation

```bash
go get github.com/cuprite-io/assay
```

---

## 🚀 Public API Design

Assay exposes a minimal public surface, keeping internal data structures and parsing state private.

### Sampler & Config
To run Assay, construct a `Sampler` passing a `StatsBackend` and configuration:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/cuprite-io/assay"
)

func main() {
	// 1. Initialize your storage backend (e.g., Capacitor, Redis, In-Memory)
	backend := NewMyCustomBackend()

	// 2. Configure Sampler limits
	cfg := assay.Config{
		MaxDepth:      32,         // Nesting limit to prevent stack exhaustion
		MaxPaths:      1000,       // Max unique paths to prevent cardinality explosion (OOM)
		FlushInterval: 100 * time.Millisecond, // Accumulation window for local writes
	}

	sampler, err := assay.NewSampler(backend, cfg)
	if err != nil {
		panic(err)
	}
	defer sampler.Close()

	ctx := context.Background()

	// 3. Sample incoming payloads
	jsonBytes := []byte(`{"user": {"name": "Alice", "tags": ["admin"]}}`)
	err := sampler.Sample(ctx, "api-endpoint-v1", jsonBytes)
	if err != nil {
		panic(err)
	}

	// 4. Retrieve computed schema tree on-demand
	schema, err := sampler.GetSchema(ctx, "api-endpoint-v1")
	if err != nil {
		panic(err)
	}
	fmt.Printf("Dominant Type of user.name: %s\n", schema.Children["user"].Children["name"].Type)
}
```

---

## 🔌 Decoupled Storage Interface (`StatsBackend`)

To plug Assay into your cache layer, implement the `StatsBackend` interface. Because Go utilizes structural typing, any struct implementing `MapIncrementBy`, `MapGetAll`, and `Delete` fits this contract automatically.

```go
type StatsBackend interface {
	MapIncrementBy(ctx context.Context, key, field string, delta float64) (float64, error)
	MapGetAll(ctx context.Context, key string) (map[string]string, error)
	Delete(ctx context.Context, key string) error
}
```

### Direct Integration with Capacitor
Since [Capacitor](file:///home/shantanu/Projects/cuprite-io/capacitor/capacitor.go) already implements `MapIncrementBy` and `MapGetAll` with these exact signatures, a `*capacitor.Capacitor` instance fits this contract **out of the box**. 

No adapter or wrapper class is needed:

```go
import (
	"context"
	"github.com/cuprite-io/assay"
	"github.com/cuprite-io/capacitor"
)

func main() {
	// Initialize Capacitor client
	capClient, _ := capacitor.New(...)

	// Pass capClient directly as the backend!
	sampler, err := assay.NewSampler(capClient, assay.Config{})
	if err != nil {
		panic(err)
	}
	defer sampler.Close()
	
	// ...
}
```

---

## 🧮 Mathematical Model

### Type Probability
For a path $p$, the probability of type $T$ is computed as:
$$P(T \mid p) = \frac{\text{Count}(p, T)}{\sum_{T' \in \mathcal{T}} \text{Count}(p, T')}$$

### Field Requirement
A field $p$ is marked as `required` (relative to its parent) if it is observed every time its parent object is present, and its value is never `null`:
$$\text{IsRequired}(p) = \left( \text{Observations}(p) == \text{Observations}(parent(p)) \right) \land \left( \text{Count}(p, \text{Null}) == 0 \right)$$

---

## 📄 License

Assay is licensed under the Apache License, Version 2.0. See [LICENSE](file:///home/shantanu/Projects/cuprite-io/assay/LICENSE) for the full text.
