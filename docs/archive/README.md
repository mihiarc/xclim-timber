# Documentation Archive

This directory contains historical documentation from the xclim-timber development process. These documents are preserved for reference but are **no longer current**.

## Status: Historical Reference Only

**Current Project Status:** 80/80 indices implemented (100% complete) as of 2025-10-10

**For Current Documentation:** See [../README.md](../README.md) and [../ACHIEVABLE_INDICES_ROADMAP.md](../ACHIEVABLE_INDICES_ROADMAP.md)

## Archived Documents

### Planning Documents (Phases 1-10)

| Document | Original Purpose | Time Period | Status |
|----------|------------------|-------------|---------|
| `planning_phases_1-7.md` | Gap analysis and implementation planning | Mid-project (~36/84 complete) | Phases complete |
| `phase5_implementation_notes.md` | Technical notes for multivariate indices | Phase 5 (46→50 indices) | Phase complete |

### Outdated Reference Documents

| Document | Original Purpose | Last Accurate | Reason Archived |
|----------|------------------|---------------|-----------------|
| `ADDITIONAL_INDICES_AVAILABLE.md` | Catalog of xclim indicators | Phase 5 (50 indices) | Stopped updating after Phase 5 |
| `CLIMATE_INDICES_REFERENCE_v1.md` | Index reference guide | Early project (26 indices) | Severely outdated (missing 54 indices) |
| `DATA_DICTIONARY_v1.md` | Variable definitions | Mid-project (42 indices) | Incomplete (missing 38 indices) |

## Why These Documents Are Archived

These documents served important roles during development but are now **superseded** by:

1. **[ACHIEVABLE_INDICES_ROADMAP.md](../ACHIEVABLE_INDICES_ROADMAP.md)** - Comprehensive, current index catalog (80 indices)
2. **[CLAUDE.md](../CLAUDE.md)** - Updated system architecture and overview
3. **[PRODUCTION_GUIDE.md](../../PRODUCTION_GUIDE.md)** - Production workflow documentation
4. **[BASELINE_DOCUMENTATION.md](../BASELINE_DOCUMENTATION.md)** - Current baseline methodology

## Historical Value

These documents provide insight into:
- **Planning process**: How phases were organized and prioritized
- **Technical challenges**: Implementation notes and solutions
- **Evolution**: Project growth from 26 → 80 indices over time
- **Decision-making**: Why certain indices were prioritized

## Implementation Timeline (for Historical Context)

| Phase | Indices Added | Progress | Date | Document |
|-------|---------------|----------|------|----------|
| Initial | 26 | 26/84 (31%) | Early 2025 | CLIMATE_INDICES_REFERENCE_v1.md |
| Phase 1-3 | +10 | 36/84 (43%) | Mid 2025 | planning_phases_1-7.md |
| Phase 4 | +3 | 39/84 (46%) | 2025-10 | - |
| Phase 5 | +4 | 43/84 (51%) | 2025-10 | phase5_implementation_notes.md |
| **Phase 6** | +3 | 46/84 (55%) | 2025-10-10 | See roadmap |
| **Phase 7** | +8 | 54/84 (64%) | 2025-10-10 | See roadmap |
| **Phase 8** | +5 | 59/84 (70%) | 2025-10-10 | See roadmap |
| **Phase 9** | +2 | 61/84 (73%) | 2025-10-10 | See roadmap |
| **Phase 10** | +12 | **73/80 (100%)** | 2025-10-10 | See roadmap |

**Note:** Goal revised from 84 → 80 indices based on available PRISM data (missing wind, snow, solar variables).

## Lessons Learned (from archived documents)

### From Phase 5 Implementation Notes
- Baseline percentiles must be pre-calculated (cannot compute from small chunks)
- Coordinate alignment critical when merging multiple zarr stores
- xclim functions expect DataArrays with 'dayofyear' dimension for percentiles

### From Planning Documents
- Prioritize "quick wins" (low effort, high value) for momentum
- Group indices by data requirements to minimize pipeline complexity
- Fixed-threshold indices easier than percentile-based (no baseline updates)

### From Reference Document Evolution
- Documentation drift is real - multiple docs with conflicting numbers
- Single source of truth (roadmap) prevents confusion
- Regular updates critical as project progresses

## Do Not Use for Current Work

⚠️ **Warning:** These documents contain **outdated information**. Do not use them for:
- Understanding current system capabilities
- Planning new work
- Documenting current status
- Troubleshooting

**Instead, use the current documentation** linked at the top of this README.

---

**Archive Created:** 2025-10-11
**Archive Maintainer:** Development team
**Last Updated:** 2025-10-11
