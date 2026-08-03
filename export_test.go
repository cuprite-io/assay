package assay

// ExportBuildSchemaTree exposes the private buildSchemaTree function for external package tests.
func ExportBuildSchemaTree(statsMap map[string]*PathStatsSnapshot, totalPayloads uint64) *SchemaNode {
	return buildSchemaTree(statsMap, totalPayloads)
}

// Config returns the configuration of the sampler for testing purposes.
func (s *Sampler) Config() Config {
	return s.config
}
