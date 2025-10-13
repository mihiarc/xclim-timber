# GitHub Issues for Pipeline Refactoring

This directory contains 12 issues for systematic pipeline refactoring to eliminate ~2,800 lines of duplicate code.

## Quick Upload to GitHub

You can create these issues using the GitHub CLI (`gh`):

```bash
# Install gh if needed
# sudo apt install gh  # or: brew install gh

# Authenticate
gh auth login

# Create all issues
for file in issue-*.md; do
  gh issue create --title "$(head -n1 $file | sed 's/# //')" --body-file "$file"
done
```

Or manually create them through the GitHub web interface.

## Issue Summary

| # | Title | Priority | Estimate | Dependencies |
|---|-------|----------|----------|--------------|
| 1 | Create core module structure and base pipeline class | High | 4h | None |
| 2 | Create shared configuration and utilities | High | 2h | #1 |
| 3 | Refactor temperature_pipeline.py | High | 3h | #1, #2 |
| 4 | Refactor precipitation_pipeline.py | High | 2h | #3 |
| 5 | Refactor drought_pipeline.py | Medium | 2.5h | #4 |
| 6 | Refactor agricultural_pipeline.py | Medium | 2h | #5 |
| 7 | Refactor humidity_pipeline.py | Low | 1.5h | #6 |
| 8 | Refactor human_comfort_pipeline.py | Low | 1.5h | #7 |
| 9 | Refactor multivariate_pipeline.py | Low | 2h | #8 |
| 10 | Integration testing and validation | High | 3h | #3-9 |
| 11 | Update documentation | Medium | 2h | #10 |
| 12 | Cleanup and archive old code | Low | 1h | #11 |

**Total Estimated Time:** 26-28 hours

## Critical Path

The critical path is: **#1 → #2 → #3 → #10**

These issues must be completed in order. Other issues (#4-9) can be parallelized after #3.

## Benefits

- **-49%** total lines of code (4,402 → ~2,230)
- **-100%** duplicate code (~2,800 lines eliminated)
- Easier maintenance - single place to fix bugs
- Easier to add new pipelines - inherit from BasePipeline
- Consistent behavior across all pipelines

## Breaking Changes

- Internal API changes (not user-facing)
- CLI remains compatible (same flags)
- Output format unchanged (same NetCDF structure)
- **No backward compatibility** - clean break from old architecture
