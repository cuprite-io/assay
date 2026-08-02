package assay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// DataType represents the primitive JSON types.
type DataType int

const (
	TypeNull DataType = iota
	TypeString
	TypeNumber
	TypeBoolean
	TypeObject
	TypeArray
)

// String returns the string representation of DataType.
func (d DataType) String() string {
	switch d {
	case TypeNull:
		return "null"
	case TypeString:
		return "string"
	case TypeNumber:
		return "number"
	case TypeBoolean:
		return "boolean"
	case TypeObject:
		return "object"
	case TypeArray:
		return "array"
	default:
		return "unknown"
	}
}

// SchemaNode represents a reconstructed node in the schema tree.
type SchemaNode struct {
	Name        string                 `json:"name"`
	Path        string                 `json:"path"`
	Type        string                 `json:"type"`        // "string", "number", "mixed", "null", "object", etc.
	Required    bool                   `json:"required"`    // Evaluated against parent observation frequency
	Probability map[string]float64     `json:"probability"` // E.g., {"string": 0.95, "number": 0.05}
	Children    map[string]*SchemaNode `json:"children,omitempty"`
}

// PathUpdate represents an aggregated increment update for a path and type.
type PathUpdate struct {
	Path  string
	Type  DataType
	Count uint64
}

// PathStatsSnapshot represents the snapshot of a path's counters.
type PathStatsSnapshot struct {
	ObservedCount uint64
	TypeCounts    [6]uint64
}

// StatsBackend represents the abstraction layer for stats storage.
// It matches Capacitor's MapIncrementBy and MapGetAll methods, enabling implicit integration.
type StatsBackend interface {
	MapIncrementBy(ctx context.Context, key, field string, delta float64) (float64, error)
	MapGetAll(ctx context.Context, key string) (map[string]string, error)
}

// Config defines execution and limits for safety.
type Config struct {
	MaxDepth      int           // Prevents Stack Overflow (Default: 32)
	MaxPaths      int           // Prevents Cardinality Explosion (Default: 1000)
	FlushInterval time.Duration // Interval for buffering writes to stats backend (Default: 100ms)
}

// Sampler is the entrypoint coordinator.
type Sampler struct {
	backend StatsBackend
	config  Config

	shards []*schemaShard
	pool   sync.Pool

	quit chan struct{}
	wg   sync.WaitGroup
}

type schemaShard struct {
	mu      sync.RWMutex
	schemas map[string]*schemaAccumulator
}

type schemaAccumulator struct {
	mu    sync.RWMutex
	stats map[string]*pathStats
}

type pathStats struct {
	types [6]uint64
}

const numShards = 128

// NewSampler initializes a new Sampler with a backend and configuration.
func NewSampler(backend StatsBackend, cfg Config) *Sampler {
	if backend == nil {
		panic("assay: backend cannot be nil")
	}
	if cfg.MaxDepth <= 0 {
		cfg.MaxDepth = 32
	}
	if cfg.MaxPaths <= 0 {
		cfg.MaxPaths = 1000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}

	shards := make([]*schemaShard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &schemaShard{
			schemas: make(map[string]*schemaAccumulator),
		}
	}

	s := &Sampler{
		backend: backend,
		config:  cfg,
		shards:  shards,
		pool: sync.Pool{
			New: func() any {
				return &pathStack{
					buf:     make([]byte, 0, 256),
					offsets: make([]int, 0, 16),
				}
			},
		},
		quit: make(chan struct{}),
	}

	s.wg.Add(1)
	go s.flushLoop()

	return s
}

// Close flushes any pending stats and shuts down the background flushing worker.
func (s *Sampler) Close() error {
	close(s.quit)
	s.wg.Wait()
	return nil
}

// Sample parses the incoming payload and updates stats in the local buffer.
// payload can be: []byte (JSON), map[string]any, or a Struct pointer.
func (s *Sampler) Sample(ctx context.Context, schemaID string, payload any) error {
	if payload == nil {
		return errors.New("nil payload")
	}

	stack := s.pool.Get().(*pathStack)
	stack.reset()
	defer s.pool.Put(stack)

	sa := s.getSchemaAccumulator(schemaID, true)

	// Callback to increment local path statistics
	recordStats := func(path string, dt DataType) {
		sa.mu.RLock()
		stats, ok := sa.stats[path]
		sa.mu.RUnlock()

		if !ok {
			sa.mu.Lock()
			// Double-check under write lock
			stats, ok = sa.stats[path]
			if !ok {
				// Enforce cardinality limits to prevent OOM
				if len(sa.stats) >= s.config.MaxPaths {
					sa.mu.Unlock()
					return
				}
				stats = &pathStats{}
				sa.stats[path] = stats
			}
			sa.mu.Unlock()
		}

		atomic.AddUint64(&stats.types[dt], 1)
	}

	// 1. Ingest/parse data depending on type
	switch val := payload.(type) {
	case []byte:
		if err := parseJSON(val, stack, 0, s.config.MaxDepth, recordStats); err != nil {
			return err
		}
	default:
		// Go value, map, or struct
		if err := parseGoValue(val, stack, 0, s.config.MaxDepth, recordStats); err != nil {
			return err
		}
	}

	return nil
}

// GetSchema reconstructs and returns the computed schema tree from the backend metrics.
func (s *Sampler) GetSchema(ctx context.Context, schemaID string) (*SchemaNode, error) {
	merged := make(map[string]*PathStatsSnapshot)

	// Fetch flat snapshot from backend
	rawStats, err := s.backend.MapGetAll(ctx, schemaID)
	if err == nil {
		typeMap := map[string]DataType{
			"null":    TypeNull,
			"string":  TypeString,
			"number":  TypeNumber,
			"boolean": TypeBoolean,
			"object":  TypeObject,
			"array":   TypeArray,
		}
		for k, v := range rawStats {
			idx := strings.LastIndex(k, ":")
			if idx == -1 {
				continue
			}
			path := k[:idx]
			typeStr := k[idx+1:]

			dt, exists := typeMap[typeStr]
			if !exists {
				continue
			}

			count, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				continue
			}

			snap, ok := merged[path]
			if !ok {
				snap = &PathStatsSnapshot{}
				merged[path] = snap
			}
			snap.TypeCounts[dt] = count
		}
		// Compute ObservedCount for backend metrics
		for _, snap := range merged {
			var total uint64
			for _, c := range snap.TypeCounts {
				total += c
			}
			snap.ObservedCount = total
		}
	}

	// Merge local unflushed stats
	sa := s.getSchemaAccumulator(schemaID, false)
	if sa != nil {
		sa.mu.RLock()
		for path, stats := range sa.stats {
			snap, ok := merged[path]
			if !ok {
				snap = &PathStatsSnapshot{}
				merged[path] = snap
			}
			for t := 0; t < 6; t++ {
				c := atomic.LoadUint64(&stats.types[t])
				snap.TypeCounts[t] += c
			}
			// Update ObservedCount
			var total uint64
			for _, c := range snap.TypeCounts {
				total += c
			}
			snap.ObservedCount = total
		}
		sa.mu.RUnlock()
	}

	// Fetch total schema observations using root path ""
	var totalPayloads uint64 = 1
	if rootSnap, exists := merged[""]; exists {
		totalPayloads = rootSnap.ObservedCount
	}

	return buildSchemaTree(merged, totalPayloads), nil
}

// fnvHash computes FNV-1a hash of a string to select a shard.
func fnvHash(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func (s *Sampler) getSchemaAccumulator(schemaID string, createIfMissing bool) *schemaAccumulator {
	h := fnvHash(schemaID) % numShards
	shard := s.shards[h]

	shard.mu.RLock()
	sa, ok := shard.schemas[schemaID]
	shard.mu.RUnlock()

	if !ok && createIfMissing {
		shard.mu.Lock()
		sa, ok = shard.schemas[schemaID]
		if !ok {
			sa = &schemaAccumulator{
				stats: make(map[string]*pathStats),
			}
			shard.schemas[schemaID] = sa
		}
		shard.mu.Unlock()
	}

	return sa
}

func (s *Sampler) flushLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.quit:
			s.flushAll()
			return
		case <-ticker.C:
			s.flushAll()
		}
	}
}

func (s *Sampler) flushAll() {
	ctx := context.Background()
	typeNames := [6]string{"null", "string", "number", "boolean", "object", "array"}

	for _, shard := range s.shards {
		shard.mu.RLock()
		schemas := make(map[string]*schemaAccumulator, len(shard.schemas))
		for k, v := range shard.schemas {
			schemas[k] = v
		}
		shard.mu.RUnlock()

		for schemaID, sa := range schemas {
			sa.mu.RLock()
			type fieldUpdate struct {
				field string
				count uint64
			}
			var updates []fieldUpdate
			for path, stats := range sa.stats {
				for t := 0; t < 6; t++ {
					c := atomic.SwapUint64(&stats.types[t], 0)
					if c > 0 {
						field := fmt.Sprintf("%s:%s", path, typeNames[t])
						updates = append(updates, fieldUpdate{
							field: field,
							count: c,
						})
					}
				}
			}
			sa.mu.RUnlock()

			for _, u := range updates {
				_, _ = s.backend.MapIncrementBy(ctx, schemaID, u.field, float64(u.count))
			}
		}
	}
}

// pathStack is a reusable byte slice stack for constructing paths without allocations.
type pathStack struct {
	buf     []byte
	offsets []int
}

func (s *pathStack) reset() {
	s.buf = s.buf[:0]
	s.offsets = s.offsets[:0]
}

func (s *pathStack) push(key []byte) {
	s.offsets = append(s.offsets, len(s.buf))
	if len(s.buf) > 0 {
		s.buf = append(s.buf, '.')
	}
	s.buf = append(s.buf, key...)
}

func (s *pathStack) pop() {
	if len(s.offsets) == 0 {
		return
	}
	lastIdx := len(s.offsets) - 1
	offset := s.offsets[lastIdx]
	s.buf = s.buf[:offset]
	s.offsets = s.offsets[:lastIdx]
}

func (s *pathStack) current() string {
	return string(s.buf)
}

// ErrMaxDepthExceeded is returned when the payload exceeds the maximum depth limit.
var ErrMaxDepthExceeded = errors.New("maximum JSON nesting depth exceeded")

// parseJSON processes raw JSON bytes and extracts paths.
func parseJSON(data []byte, stack *pathStack, depth, maxDepth int, callback func(path string, dt DataType)) error {
	_, err := parseValue(data, 0, stack, depth, maxDepth, callback)
	return err
}

func skipWhitespace(data []byte, pos int) int {
	for pos < len(data) {
		c := data[pos]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			pos++
		} else {
			break
		}
	}
	return pos
}

func parseValue(data []byte, pos int, stack *pathStack, depth, maxDepth int, callback func(path string, dt DataType)) (int, error) {
	if depth > maxDepth {
		return pos, ErrMaxDepthExceeded
	}
	pos = skipWhitespace(data, pos)
	if pos >= len(data) {
		return pos, io.ErrUnexpectedEOF
	}

	switch data[pos] {
	case '{':
		callback(stack.current(), TypeObject)
		return parseObject(data, pos, stack, depth, maxDepth, callback)
	case '[':
		callback(stack.current(), TypeArray)
		return parseArray(data, pos, stack, depth, maxDepth, callback)
	case '"':
		callback(stack.current(), TypeString)
		return skipString(data, pos)
	case 't', 'f':
		callback(stack.current(), TypeBoolean)
		return skipBool(data, pos)
	case 'n':
		callback(stack.current(), TypeNull)
		return skipNull(data, pos)
	default:
		if (data[pos] >= '0' && data[pos] <= '9') || data[pos] == '-' {
			callback(stack.current(), TypeNumber)
			return skipNumber(data, pos)
		}
		return pos, fmt.Errorf("invalid json character at pos %d: %q", pos, data[pos])
	}
}

func parseObject(data []byte, pos int, stack *pathStack, depth, maxDepth int, callback func(path string, dt DataType)) (int, error) {
	if depth > maxDepth {
		return pos, ErrMaxDepthExceeded
	}
	pos++ // skip '{'

	for {
		pos = skipWhitespace(data, pos)
		if pos >= len(data) {
			return pos, io.ErrUnexpectedEOF
		}
		if data[pos] == '}' {
			pos++
			return pos, nil
		}

		if data[pos] != '"' {
			return pos, fmt.Errorf("expected string key at pos %d, got %q", pos, data[pos])
		}

		endKey, key, err := readString(data, pos)
		if err != nil {
			return pos, err
		}
		pos = endKey

		pos = skipWhitespace(data, pos)
		if pos >= len(data) || data[pos] != ':' {
			return pos, fmt.Errorf("expected ':' at pos %d", pos)
		}
		pos++ // skip ':'

		stack.push(key)
		nextPos, err := parseValue(data, pos, stack, depth+1, maxDepth, callback)
		if err != nil {
			stack.pop()
			return nextPos, err
		}
		pos = nextPos
		stack.pop()

		pos = skipWhitespace(data, pos)
		if pos >= len(data) {
			return pos, io.ErrUnexpectedEOF
		}
		if data[pos] == ',' {
			pos++
			continue
		}
		if data[pos] == '}' {
			pos++
			return pos, nil
		}
		return pos, fmt.Errorf("expected ',' or '}' at pos %d, got %q", pos, data[pos])
	}
}

func parseArray(data []byte, pos int, stack *pathStack, depth, maxDepth int, callback func(path string, dt DataType)) (int, error) {
	if depth > maxDepth {
		return pos, ErrMaxDepthExceeded
	}
	pos++ // skip '['

	stack.push([]byte("*"))
	defer stack.pop()

	for {
		pos = skipWhitespace(data, pos)
		if pos >= len(data) {
			return pos, io.ErrUnexpectedEOF
		}
		if data[pos] == ']' {
			pos++
			return pos, nil
		}

		nextPos, err := parseValue(data, pos, stack, depth+1, maxDepth, callback)
		if err != nil {
			return nextPos, err
		}
		pos = nextPos

		pos = skipWhitespace(data, pos)
		if pos >= len(data) {
			return pos, io.ErrUnexpectedEOF
		}
		if data[pos] == ',' {
			pos++
			continue
		}
		if data[pos] == ']' {
			pos++
			return pos, nil
		}
		return pos, fmt.Errorf("expected ',' or ']' at pos %d, got %q", pos, data[pos])
	}
}

func readString(data []byte, pos int) (int, []byte, error) {
	pos++ // skip leading '"'
	start := pos
	for pos < len(data) {
		if data[pos] == '"' {
			return pos + 1, data[start:pos], nil
		}
		if data[pos] == '\\' {
			pos += 2
		} else {
			pos++
		}
	}
	return pos, nil, io.ErrUnexpectedEOF
}

func skipString(data []byte, pos int) (int, error) {
	pos++
	for pos < len(data) {
		if data[pos] == '"' {
			return pos + 1, nil
		}
		if data[pos] == '\\' {
			pos += 2
		} else {
			pos++
		}
	}
	return pos, io.ErrUnexpectedEOF
}

func skipBool(data []byte, pos int) (int, error) {
	switch data[pos] {
	case 't':
		if pos+4 <= len(data) && string(data[pos:pos+4]) == "true" {
			return pos + 4, nil
		}
	case 'f':
		if pos+5 <= len(data) && string(data[pos:pos+5]) == "false" {
			return pos + 5, nil
		}
	}
	return pos, fmt.Errorf("expected boolean at pos %d", pos)
}

func skipNull(data []byte, pos int) (int, error) {
	if pos+4 <= len(data) && string(data[pos:pos+4]) == "null" {
		return pos + 4, nil
	}
	return pos, fmt.Errorf("expected null at pos %d", pos)
}

func skipNumber(data []byte, pos int) (int, error) {
	for pos < len(data) {
		c := data[pos]
		if (c >= '0' && c <= '9') || c == '.' || c == '-' || c == '+' || c == 'e' || c == 'E' {
			pos++
		} else {
			break
		}
	}
	return pos, nil
}

func parseGoValue(val any, stack *pathStack, depth, maxDepth int, callback func(path string, dt DataType)) error {
	return reflectTraverse(reflect.ValueOf(val), stack, depth, maxDepth, callback)
}

func reflectTraverse(v reflect.Value, stack *pathStack, depth, maxDepth int, callback func(path string, dt DataType)) error {
	if depth > maxDepth {
		return ErrMaxDepthExceeded
	}

	if !v.IsValid() {
		callback(stack.current(), TypeNull)
		return nil
	}

	// De-reference pointers
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			callback(stack.current(), TypeNull)
			return nil
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Struct:
		callback(stack.current(), TypeObject)
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			field := t.Field(i)
			if field.PkgPath != "" { // unexported
				continue
			}
			name := field.Name
			if jsonTag := field.Tag.Get("json"); jsonTag != "" {
				parts := strings.Split(jsonTag, ",")
				if parts[0] == "-" {
					continue
				}
				if parts[0] != "" {
					name = parts[0]
				}
			}
			stack.push([]byte(name))
			if err := reflectTraverse(v.Field(i), stack, depth+1, maxDepth, callback); err != nil {
				stack.pop()
				return err
			}
			stack.pop()
		}
	case reflect.Map:
		callback(stack.current(), TypeObject)
		for _, key := range v.MapKeys() {
			if key.Kind() == reflect.String {
				stack.push([]byte(key.String()))
				if err := reflectTraverse(v.MapIndex(key), stack, depth+1, maxDepth, callback); err != nil {
					stack.pop()
					return err
				}
				stack.pop()
			}
		}
	case reflect.Slice, reflect.Array:
		callback(stack.current(), TypeArray)
		stack.push([]byte("*"))
		for i := 0; i < v.Len(); i++ {
			if err := reflectTraverse(v.Index(i), stack, depth+1, maxDepth, callback); err != nil {
				stack.pop()
				return err
			}
		}
		stack.pop()
	case reflect.String:
		callback(stack.current(), TypeString)
	case reflect.Bool:
		callback(stack.current(), TypeBoolean)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		callback(stack.current(), TypeNumber)
	case reflect.Interface:
		if v.IsNil() {
			callback(stack.current(), TypeNull)
		} else {
			if err := reflectTraverse(v.Elem(), stack, depth+1, maxDepth, callback); err != nil {
				return err
			}
		}
	default:
		callback(stack.current(), TypeNull)
	}

	return nil
}

func buildSchemaTree(statsMap map[string]*PathStatsSnapshot, totalPayloads uint64) *SchemaNode {
	if len(statsMap) == 0 {
		return &SchemaNode{
			Name:        "root",
			Path:        "",
			Type:        "object",
			Required:    true,
			Probability: map[string]float64{"object": 1.0},
		}
	}

	nodes := make(map[string]*SchemaNode)

	root := &SchemaNode{
		Name:        "root",
		Path:        "",
		Type:        "object",
		Required:    true,
		Probability: map[string]float64{"object": 1.0},
		Children:    make(map[string]*SchemaNode),
	}
	nodes[""] = root

	paths := make([]string, 0, len(statsMap))
	for p := range statsMap {
		if p != "" {
			paths = append(paths, p)
		}
	}
	sort.Strings(paths)

	for _, path := range paths {
		parts := strings.Split(path, ".")

		currPath := ""
		for _, part := range parts {
			parentPath := currPath
			if currPath == "" {
				currPath = part
			} else {
				currPath = currPath + "." + part
			}

			if _, ok := nodes[currPath]; !ok {
				node := &SchemaNode{
					Name:     part,
					Path:     currPath,
					Children: make(map[string]*SchemaNode),
				}
				nodes[currPath] = node

				parent := nodes[parentPath]
				if parent.Children == nil {
					parent.Children = make(map[string]*SchemaNode)
				}
				parent.Children[part] = node
			}
		}
	}

	typeNames := [6]string{"null", "string", "number", "boolean", "object", "array"}

	for path, node := range nodes {
		if path == "" {
			continue
		}

		snap, ok := statsMap[path]
		if !ok {
			node.Type = "object"
			node.Probability = map[string]float64{"object": 1.0}
			node.Required = true
			continue
		}

		if snap.ObservedCount == 0 {
			node.Type = "null"
			node.Probability = map[string]float64{"null": 1.0}
			node.Required = false
			continue
		}

		node.Probability = make(map[string]float64)
		var maxCount uint64
		var dominantType string
		var activeTypesCount int

		for t, count := range snap.TypeCounts {
			if count > 0 {
				prob := float64(count) / float64(snap.ObservedCount)
				node.Probability[typeNames[t]] = prob

				if count > maxCount {
					maxCount = count
					dominantType = typeNames[t]
				}
				activeTypesCount++
			}
		}

		if activeTypesCount > 1 {
			node.Type = "mixed"
		} else {
			node.Type = dominantType
		}

		parentPath := ""
		if idx := strings.LastIndex(path, "."); idx != -1 {
			parentPath = path[:idx]
		}

		parentObserved := totalPayloads
		if parentPath != "" {
			if parentSnap, exists := statsMap[parentPath]; exists {
				parentObserved = parentSnap.ObservedCount
			}
		}

		node.Required = (snap.ObservedCount == parentObserved) && (snap.TypeCounts[TypeNull] == 0)
	}

	for _, node := range nodes {
		if len(node.Children) == 0 {
			node.Children = nil
		}
	}

	return root
}
