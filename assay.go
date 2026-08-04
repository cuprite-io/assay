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

// Version is the current version of the assay library.
const Version = "1.0.0"

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
		return fmt.Sprintf("DataType(%d)", d)
	}
}

// SchemaNode represents a reconstructed node in the schema tree.
// Note: This structure is not safe for concurrent mutation. If multiple
// goroutines write to the returned tree, external synchronization is required.
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
// It matches Capacitor's MapIncrementBy, MapGetAll, and Delete methods, enabling implicit integration.
type StatsBackend interface {
	MapIncrementBy(ctx context.Context, key, field string, delta float64) (float64, error)
	MapGetAll(ctx context.Context, key string) (map[string]string, error)
	Delete(ctx context.Context, key string) error
}

// Config defines execution and limits for safety.
type Config struct {
	MaxDepth         int           // Prevents Stack Overflow (Default: 32)
	MaxPaths         int           // Prevents Cardinality Explosion (Default: 1000)
	MaxSchemas       int           // Prevents unbounded schema creation (Default: 1000)
	MaxArrayElements int           // Max array/slice elements profiled (Default: 10)
	FlushInterval    time.Duration // Interval for buffering writes to stats backend (Default: 100ms. Set to -1 to disable periodic background flushing)
	FlushTimeout     time.Duration // Timeout for individual backend writes (Default: 3s)
	OnError          func(error)   // Optional callback to handle flush or background errors
}

// Sampler is the entrypoint coordinator.
// Sampler must not be copied after creation.
type Sampler struct {
	backend StatsBackend
	config  Config

	shards []*schemaShard
	pool   sync.Pool

	wg sync.WaitGroup

	activeSchemas int64

	closed       atomic.Bool
	ctx          context.Context
	cancel       context.CancelFunc
	lastFlushErr atomic.Value

	ingestedSamples uint64
	droppedSamples  uint64
	rejectedSamples uint64
	flushSuccesses  uint64
	flushFailures   uint64
}

type schemaShard struct {
	mu      sync.RWMutex
	schemas map[string]*schemaAccumulator
}

type schemaAccumulator struct {
	mu      sync.RWMutex
	stats   map[string]*pathStats
	deleted atomic.Bool
	flushMu sync.Mutex
}

type pathStats struct {
	types [6]uint64
}

const numShards = 128

// NewSampler initializes a new Sampler with a backend and configuration.
func NewSampler(backend StatsBackend, cfg Config) (*Sampler, error) {
	if backend == nil {
		return nil, errors.New("assay: backend cannot be nil")
	}
	if cfg.MaxDepth < 0 || cfg.MaxDepth > 256 {
		return nil, fmt.Errorf("assay: invalid MaxDepth %d (must be between 0 and 256)", cfg.MaxDepth)
	}
	if cfg.MaxPaths < 0 || cfg.MaxPaths > 100_000 {
		return nil, fmt.Errorf("assay: invalid MaxPaths %d (must be between 0 and 100,000)", cfg.MaxPaths)
	}
	if cfg.MaxSchemas < 0 || cfg.MaxSchemas > 100_000 {
		return nil, fmt.Errorf("assay: invalid MaxSchemas %d (must be between 0 and 100,000)", cfg.MaxSchemas)
	}
	if cfg.MaxArrayElements < 0 || cfg.MaxArrayElements > 10000 {
		return nil, fmt.Errorf("assay: invalid MaxArrayElements %d (must be between 0 and 10,000)", cfg.MaxArrayElements)
	}
	if (cfg.FlushInterval < 0 && cfg.FlushInterval != -1) || cfg.FlushInterval > 60*time.Second {
		return nil, fmt.Errorf("assay: invalid FlushInterval %v (must be -1 or between 0 and 60 seconds)", cfg.FlushInterval)
	}
	if cfg.FlushTimeout < 0 || cfg.FlushTimeout > 60*time.Second {
		return nil, fmt.Errorf("assay: invalid FlushTimeout %v (must be between 0 and 60 seconds)", cfg.FlushTimeout)
	}

	// Apply defaults for zero values
	if cfg.MaxDepth == 0 {
		cfg.MaxDepth = 32
	}
	if cfg.MaxPaths == 0 {
		cfg.MaxPaths = 1000
	}
	if cfg.MaxSchemas == 0 {
		cfg.MaxSchemas = 1000
	}
	if cfg.MaxArrayElements == 0 {
		cfg.MaxArrayElements = 10
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	if cfg.FlushTimeout == 0 {
		cfg.FlushTimeout = 3 * time.Second
	}

	shards := make([]*schemaShard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &schemaShard{
			schemas: make(map[string]*schemaAccumulator),
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

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
		ctx:    ctx,
		cancel: cancel,
	}

	s.wg.Add(1)
	go s.flushLoop()

	return s, nil
}

type lastErr struct {
	err error
}

func (s *Sampler) loadLastFlushErr() error {
	val := s.lastFlushErr.Load()
	if val == nil {
		return nil
	}
	return val.(lastErr).err
}

func (s *Sampler) storeLastFlushErr(err error) {
	s.lastFlushErr.Store(lastErr{err: err})
}

// Close flushes any pending stats and shuts down the background flushing worker.
// It returns an error if the shutdown or final flush timed out or failed.
func (s *Sampler) Close() error {
	if s.closed.Swap(true) {
		return errors.New("assay: sampler already closed")
	}
	s.cancel()

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return s.loadLastFlushErr()
	case <-time.After(5 * time.Second):
		return errors.New("assay: close timed out waiting for background flush to finish")
	}
}

// Flush forces an immediate synchronous flush of all locally accumulated statistics to the backend.
// It returns an error if any flush operation failed.
func (s *Sampler) Flush() error {
	if s.closed.Load() {
		return ErrClosed
	}
	return s.flushAll(s.ctx)
}

// SamplerStats contains metrics and observability data for the Sampler.
type SamplerStats struct {
	ActiveSchemas   int64
	IngestedSamples uint64
	DroppedSamples  uint64
	RejectedSamples uint64
	FlushSuccesses  uint64
	FlushFailures   uint64
}

// Stats returns a snapshot of the Sampler's metrics.
func (s *Sampler) Stats() SamplerStats {
	return SamplerStats{
		ActiveSchemas:   atomic.LoadInt64(&s.activeSchemas),
		IngestedSamples: atomic.LoadUint64(&s.ingestedSamples),
		DroppedSamples:  atomic.LoadUint64(&s.droppedSamples),
		RejectedSamples: atomic.LoadUint64(&s.rejectedSamples),
		FlushSuccesses:  atomic.LoadUint64(&s.flushSuccesses),
		FlushFailures:   atomic.LoadUint64(&s.flushFailures),
	}
}

// DeleteSchema removes a schema ID and all its accumulated stats from local memory
// and deletes the schema's metrics from the persistent stats backend.
func (s *Sampler) DeleteSchema(ctx context.Context, schemaID string) error {
	h := fnvHash(schemaID) % numShards
	shard := s.shards[h]
	shard.mu.Lock()
	sa, exists := shard.schemas[schemaID]
	if exists {
		sa.deleted.Store(true)
		delete(shard.schemas, schemaID)
		for {
			currentActive := atomic.LoadInt64(&s.activeSchemas)
			nextActive := currentActive - 1
			if nextActive < 0 {
				nextActive = 0
			}
			if atomic.CompareAndSwapInt64(&s.activeSchemas, currentActive, nextActive) {
				break
			}
		}
	}
	shard.mu.Unlock()

	if exists {
		sa.flushMu.Lock()
		defer sa.flushMu.Unlock()
	}

	return s.backend.Delete(ctx, schemaID)
}

// Sample parses the incoming payload and updates stats in the local buffer.
// payload can be: []byte (JSON), map[string]any, or a Struct pointer.
func (s *Sampler) Sample(ctx context.Context, schemaID string, payload any) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if payload == nil {
		atomic.AddUint64(&s.rejectedSamples, 1)
		return errors.New("nil payload")
	}

	stack := s.pool.Get().(*pathStack)
	stack.reset()
	defer s.pool.Put(stack)

	sa := s.getSchemaAccumulator(schemaID, true)
	if sa == nil {
		return ErrMaxSchemasExceeded
	}

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
					atomic.AddUint64(&s.droppedSamples, 1)
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
		if err := parseJSON(val, stack, 0, s.config.MaxDepth, s.config.MaxArrayElements, recordStats); err != nil {
			atomic.AddUint64(&s.rejectedSamples, 1)
			return err
		}
	case string:
		if err := parseJSON([]byte(val), stack, 0, s.config.MaxDepth, s.config.MaxArrayElements, recordStats); err != nil {
			atomic.AddUint64(&s.rejectedSamples, 1)
			return err
		}
	default:
		// Go value, map, or struct
		if err := parseGoValue(val, stack, 0, s.config.MaxDepth, s.config.MaxArrayElements, recordStats); err != nil {
			atomic.AddUint64(&s.rejectedSamples, 1)
			return err
		}
	}

	atomic.AddUint64(&s.ingestedSamples, 1)
	return nil
}

// GetSchema reconstructs and returns the computed schema tree from the backend metrics.
func (s *Sampler) GetSchema(ctx context.Context, schemaID string) (*SchemaNode, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	merged := make(map[string]*PathStatsSnapshot)

	// Fetch flat snapshot from backend
	rawStats, err := s.backend.MapGetAll(ctx, schemaID)
	if err != nil {
		return nil, fmt.Errorf("assay: failed to fetch stats from backend: %w", err)
	}

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
			for {
				currentActive := atomic.LoadInt64(&s.activeSchemas)
				if currentActive >= int64(s.config.MaxSchemas) {
					atomic.AddUint64(&s.droppedSamples, 1)
					shard.mu.Unlock()
					return nil
				}
				if atomic.CompareAndSwapInt64(&s.activeSchemas, currentActive, currentActive+1) {
					break
				}
			}
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
	if s.config.FlushInterval == -1 {
		<-s.ctx.Done()
		err := s.flushAll(context.Background())
		s.storeLastFlushErr(err)
		return
	}

	ticker := time.NewTicker(s.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			err := s.flushAll(context.Background())
			s.storeLastFlushErr(err)
			return
		case <-ticker.C:
			_ = s.flushAll(s.ctx)
		}
	}
}

type pathStatsRef struct {
	path  string
	stats *pathStats
}

type fieldUpdate struct {
	field string
	count uint64
}

func (s *Sampler) flushAll(parentCtx context.Context) error {
	ctx, cancel := context.WithTimeout(parentCtx, s.config.FlushTimeout)
	defer cancel()

	typeNames := [6]string{"null", "string", "number", "boolean", "object", "array"}
	var errs []error

	for _, shard := range s.shards {
		shard.mu.RLock()
		schemas := make(map[string]*schemaAccumulator, len(shard.schemas))
		for k, v := range shard.schemas {
			schemas[k] = v
		}
		shard.mu.RUnlock()

		for schemaID, sa := range schemas {
			shard.mu.RLock()
			_, exists := shard.schemas[schemaID]
			shard.mu.RUnlock()
			if !exists {
				continue
			}

			if sa.deleted.Load() {
				continue
			}

			sa.mu.RLock()
			statsRefs := make([]pathStatsRef, 0, len(sa.stats))
			for path, stats := range sa.stats {
				statsRefs = append(statsRefs, pathStatsRef{path: path, stats: stats})
			}
			sa.mu.RUnlock()

			if sa.deleted.Load() {
				continue
			}

			var updates []fieldUpdate
			for _, ref := range statsRefs {
				for t := 0; t < 6; t++ {
					c := atomic.SwapUint64(&ref.stats.types[t], 0)
					if c > 0 {
						field := fmt.Sprintf("%s:%s", ref.path, typeNames[t])
						updates = append(updates, fieldUpdate{
							field: field,
							count: c,
						})
					}
				}
			}

			sa.flushMu.Lock()
			if sa.deleted.Load() {
				sa.flushMu.Unlock()
				continue
			}

			for _, u := range updates {
				if sa.deleted.Load() {
					break
				}
				_, err := s.backend.MapIncrementBy(ctx, schemaID, u.field, float64(u.count))
				if err != nil {
					atomic.AddUint64(&s.flushFailures, 1)
					flushErr := fmt.Errorf("failed to flush stats for schema %q field %q: %w", schemaID, u.field, err)
					errs = append(errs, flushErr)
					if s.config.OnError != nil {
						s.config.OnError(flushErr)
					}
				} else {
					atomic.AddUint64(&s.flushSuccesses, 1)
				}
			}
			sa.flushMu.Unlock()
		}
	}

	return errors.Join(errs...)
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
	for _, b := range key {
		switch b {
		case '.':
			s.buf = append(s.buf, '\\', '.')
		case '\\':
			s.buf = append(s.buf, '\\', '\\')
		default:
			s.buf = append(s.buf, b)
		}
	}
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

// ErrMaxSchemasExceeded is returned when the maximum number of active schemas is reached.
var ErrMaxSchemasExceeded = errors.New("maximum schema count exceeded")

// ErrClosed is returned when an operation is performed on a closed sampler.
var ErrClosed = errors.New("assay: sampler is closed")

// parseJSON processes raw JSON bytes and extracts paths.
func parseJSON(data []byte, stack *pathStack, depth, maxDepth, maxArrayElements int, callback func(path string, dt DataType)) error {
	pos := skipWhitespace(data, 0)
	_, err := parseValue(data, pos, stack, depth, maxDepth, maxArrayElements, callback)
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

func parseValue(data []byte, pos int, stack *pathStack, depth, maxDepth, maxArrayElements int, callback func(path string, dt DataType)) (int, error) {
	if depth > maxDepth {
		return pos, ErrMaxDepthExceeded
	}
	if pos >= len(data) {
		return pos, io.ErrUnexpectedEOF
	}

	switch data[pos] {
	case '{':
		if callback != nil {
			callback(stack.current(), TypeObject)
		}
		return parseObject(data, pos, stack, depth, maxDepth, maxArrayElements, callback)
	case '[':
		if callback != nil {
			callback(stack.current(), TypeArray)
		}
		return parseArray(data, pos, stack, depth, maxDepth, maxArrayElements, callback)
	case '"':
		if callback != nil {
			callback(stack.current(), TypeString)
		}
		return skipString(data, pos)
	case 't', 'f':
		if callback != nil {
			callback(stack.current(), TypeBoolean)
		}
		return skipBool(data, pos)
	case 'n':
		if callback != nil {
			callback(stack.current(), TypeNull)
		}
		return skipNull(data, pos)
	default:
		if (data[pos] >= '0' && data[pos] <= '9') || data[pos] == '-' {
			if callback != nil {
				callback(stack.current(), TypeNumber)
			}
			return skipNumber(data, pos)
		}
		return pos, fmt.Errorf("invalid json character at pos %d: %q", pos, data[pos])
	}
}

func parseObjectField(data []byte, pos int, stack *pathStack, depth, maxDepth, maxArrayElements int, key []byte, callback func(path string, dt DataType)) (int, error) {
	stack.push(key)
	defer stack.pop()
	return parseValue(data, pos, stack, depth+1, maxDepth, maxArrayElements, callback)
}

func parseObject(data []byte, pos int, stack *pathStack, depth, maxDepth, maxArrayElements int, callback func(path string, dt DataType)) (int, error) {
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
		pos = skipWhitespace(data, pos)

		nextPos, err := parseObjectField(data, pos, stack, depth, maxDepth, maxArrayElements, key, callback)
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
		if data[pos] == '}' {
			pos++
			return pos, nil
		}
		return pos, fmt.Errorf("expected ',' or '}' at pos %d, got %q", pos, data[pos])
	}
}

func parseArray(data []byte, pos int, stack *pathStack, depth, maxDepth, maxArrayElements int, callback func(path string, dt DataType)) (int, error) {
	pos++ // skip '['

	stack.push([]byte("*"))
	defer stack.pop()

	elementCount := 0
	for {
		pos = skipWhitespace(data, pos)
		if pos >= len(data) {
			return pos, io.ErrUnexpectedEOF
		}
		if data[pos] == ']' {
			pos++
			return pos, nil
		}

		cb := callback
		if elementCount >= maxArrayElements {
			cb = nil
		}

		nextPos, err := parseValue(data, pos, stack, depth+1, maxDepth, maxArrayElements, cb)
		if err != nil {
			return nextPos, err
		}
		pos = nextPos
		elementCount++

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
			if pos+1 >= len(data) {
				return len(data), nil, io.ErrUnexpectedEOF
			}
			pos += 2
		} else {
			pos++
		}
	}
	return len(data), nil, io.ErrUnexpectedEOF
}

func skipString(data []byte, pos int) (int, error) {
	pos++
	for pos < len(data) {
		if data[pos] == '"' {
			return pos + 1, nil
		}
		if data[pos] == '\\' {
			if pos+1 >= len(data) {
				return len(data), io.ErrUnexpectedEOF
			}
			pos += 2
		} else {
			pos++
		}
	}
	return len(data), io.ErrUnexpectedEOF
}

func skipBool(data []byte, pos int) (int, error) {
	switch data[pos] {
	case 't':
		if pos+4 <= len(data) && data[pos+1] == 'r' && data[pos+2] == 'u' && data[pos+3] == 'e' {
			return pos + 4, nil
		}
	case 'f':
		if pos+5 <= len(data) && data[pos+1] == 'a' && data[pos+2] == 'l' && data[pos+3] == 's' && data[pos+4] == 'e' {
			return pos + 5, nil
		}
	}
	return pos, fmt.Errorf("expected boolean at pos %d", pos)
}

func skipNull(data []byte, pos int) (int, error) {
	if pos+4 <= len(data) && data[pos] == 'n' && data[pos+1] == 'u' && data[pos+2] == 'l' && data[pos+3] == 'l' {
		return pos + 4, nil
	}
	return pos, fmt.Errorf("expected null at pos %d", pos)
}

func skipNumber(data []byte, pos int) (int, error) {
	start := pos
	if pos >= len(data) {
		return pos, io.ErrUnexpectedEOF
	}

	// 1. Optional minus sign
	if data[pos] == '-' {
		pos++
	}

	if pos >= len(data) {
		return pos, io.ErrUnexpectedEOF
	}

	// 2. Integer part
	if data[pos] == '0' {
		pos++
		if pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
			return pos, fmt.Errorf("invalid json number with leading zero starting at %d", start)
		}
	} else if data[pos] >= '1' && data[pos] <= '9' {
		pos++
		for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
			pos++
		}
	} else {
		return pos, fmt.Errorf("invalid json number starting at %d", start)
	}

	// 3. Fraction part
	if pos < len(data) && data[pos] == '.' {
		pos++
		if pos >= len(data) || data[pos] < '0' || data[pos] > '9' {
			return pos, fmt.Errorf("invalid json number fraction at %d", pos)
		}
		pos++
		for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
			pos++
		}
	}

	// 4. Exponent part
	if pos < len(data) && (data[pos] == 'e' || data[pos] == 'E') {
		pos++
		if pos < len(data) && (data[pos] == '+' || data[pos] == '-') {
			pos++
		}
		if pos >= len(data) || data[pos] < '0' || data[pos] > '9' {
			return pos, fmt.Errorf("invalid json number exponent at %d", pos)
		}
		pos++
		for pos < len(data) && data[pos] >= '0' && data[pos] <= '9' {
			pos++
		}
	}

	return pos, nil
}

func reflectTraverseField(v reflect.Value, stack *pathStack, key []byte, depth, maxDepth, maxArrayElements int, callback func(path string, dt DataType)) error {
	stack.push(key)
	defer stack.pop()
	return reflectTraverse(v, stack, depth, maxDepth, maxArrayElements, callback)
}

func reflectTraverseArray(v reflect.Value, stack *pathStack, depth, maxDepth, maxArrayElements int, callback func(path string, dt DataType)) error {
	stack.push([]byte("*"))
	defer stack.pop()
	limit := v.Len()
	if limit > maxArrayElements {
		limit = maxArrayElements
	}
	for i := 0; i < limit; i++ {
		if err := reflectTraverse(v.Index(i), stack, depth+1, maxDepth, maxArrayElements, callback); err != nil {
			return err
		}
	}
	return nil
}

func parseGoValue(val any, stack *pathStack, depth, maxDepth, maxArrayElements int, callback func(path string, dt DataType)) error {
	return reflectTraverse(reflect.ValueOf(val), stack, depth, maxDepth, maxArrayElements, callback)
}

func reflectTraverse(v reflect.Value, stack *pathStack, depth, maxDepth, maxArrayElements int, callback func(path string, dt DataType)) error {
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
			if err := reflectTraverseField(v.Field(i), stack, []byte(name), depth+1, maxDepth, maxArrayElements, callback); err != nil {
				return err
			}
		}
	case reflect.Map:
		callback(stack.current(), TypeObject)
		for _, key := range v.MapKeys() {
			if key.Kind() == reflect.String {
				if err := reflectTraverseField(v.MapIndex(key), stack, []byte(key.String()), depth+1, maxDepth, maxArrayElements, callback); err != nil {
					return err
				}
			}
		}
	case reflect.Slice, reflect.Array:
		callback(stack.current(), TypeArray)
		if err := reflectTraverseArray(v, stack, depth, maxDepth, maxArrayElements, callback); err != nil {
			return err
		}
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
			if err := reflectTraverse(v.Elem(), stack, depth, maxDepth, maxArrayElements, callback); err != nil {
				return err
			}
		}
	default:
		callback(stack.current(), TypeNull)
	}

	return nil
}

func splitPath(path string) []string {
	var parts []string
	var current strings.Builder
	inEscape := false

	for i := 0; i < len(path); i++ {
		c := path[i]
		if inEscape {
			current.WriteByte(c)
			inEscape = false
			continue
		}

		if c == '\\' {
			inEscape = true
			continue
		}

		if c == '.' {
			parts = append(parts, current.String())
			current.Reset()
		} else {
			current.WriteByte(c)
		}
	}
	parts = append(parts, current.String())
	return parts
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
		parts := splitPath(path)
		var subpaths []string
		for _, part := range parts {
			var escapedPart strings.Builder
			for i := 0; i < len(part); i++ {
				switch part[i] {
				case '.':
					escapedPart.WriteString(`\.`)
				case '\\':
					escapedPart.WriteString(`\\`)
				default:
					escapedPart.WriteByte(part[i])
				}
			}
			subpaths = append(subpaths, escapedPart.String())
		}

		var currPathBuilder strings.Builder
		for idx, part := range parts {
			escapedPart := subpaths[idx]
			parentPath := currPathBuilder.String()
			if idx > 0 {
				currPathBuilder.WriteByte('.')
			}
			currPathBuilder.WriteString(escapedPart)
			currPath := currPathBuilder.String()

			if _, ok := nodes[currPath]; !ok {
				node := &SchemaNode{
					Name: part,
					Path: currPath,
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

	return root
}
