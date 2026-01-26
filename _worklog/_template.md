# AI Agent Work Progress Record Template

This template defines the standard format for recording AI agent work sessions.

## File Naming Convention

Progress files should be named: `progress-YYYY-MM-DD-NNN.md`
- `YYYY-MM-DD`: Date of the session
- `NNN`: Sequence number for multiple sessions on the same day (001, 002, etc.)

## Template

```markdown
---
date: YYYY-MM-DD
session: <unique-id>
objective: <one-line summary>
status: completed | in-progress | blocked
---

# <Session Title>

## Objective
<What was the goal of this session>

## Completed
- <task 1>
- <task 2>

## Files Changed
- `path/to/file.rs` - <brief description>

## Tests
- Total: X passed, Y failed
- New tests: Z

## Notes
<Any important observations, decisions, or context>

## Next Steps
- <what comes next>
```

## Field Descriptions

| Field | Description |
|-------|-------------|
| `date` | Session date in ISO format |
| `session` | Unique identifier for the session |
| `objective` | One-line summary of the session goal |
| `status` | `completed`, `in-progress`, or `blocked` |

## Guidelines

1. **Be concise** - Focus on outcomes, not process
2. **List files** - Always document which files were changed
3. **Note tests** - Include test results when code changes are made
4. **Link specs** - Reference relevant specs/RFCs when applicable
5. **Next steps** - Always indicate what comes next, even if "none"
