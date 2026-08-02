# Contributing to Assay

First off, thank you for considering contributing to Assay! It's people like you that make it a great tool.

## Technical Philosophy

- **Zero-Allocation Hot Path**: The JSON stream parser and structural traversal must not allocate heap memory during ingestion. Always leverage stack allocations and pool reusable structures.
- **Lock Contention Minimization**: Write locks on statistic maps must be cold-path only. Ingestion updates must run under read locks using fast atomic operations (`sync/atomic`).
- **Decoupling**: Never bind the core sampling logic to a concrete database or remote cache client. Always program against the abstract interface.

## Development Workflow

1. **Fork the Repo**: Create a feature branch.
2. **Local Development**:
   - Ensure your code follows `go fmt`.
   - Run existing tests: `go test -v -race ./...`
3. **Testing & Benchmarks**: 
   - If you add a parser or type evaluation feature, add a unit test in `assay_test.go`.
   - Before submitting changes to the ingestion path, run benchmarks using `go test -bench=. -benchmem` to verify that performance is not degraded.
4. **Pull Request**:
   - Provide a clear description of the change.
   - Ensure all verification runs successfully.

## Code of Conduct

Be respectful and professional. We aim to build a welcoming community for everyone.

## Reporting Bugs

Use GitHub Issues to report bugs. Provide:
- A clear description of the issue.
- Steps to reproduce.
- Environment details (Go version, OS).
