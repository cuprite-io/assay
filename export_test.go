package assay

// ExportBuildSchemaTree exposes the private buildSchemaTree function for external package tests.
func ExportBuildSchemaTree(statsMap map[string]*PathStatsSnapshot, totalPayloads uint64) *SchemaNode {
	return buildSchemaTree(statsMap, totalPayloads)
}
