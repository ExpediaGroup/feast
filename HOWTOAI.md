# How to Use AI with Feast

_A practical guide for contributing to Feast using AI coding assistants_

## Core Principles

- **Human Oversight**: You are accountable for all code you submit. Never commit code you don't understand or can't maintain.
- **Quality Standards**: AI code must meet the same standards as human-written code — tests, docs, and patterns included.
- **Transparency**: Be open about significant AI usage in PRs and explain how you validated it.

## Best Practices

**✅ Recommended Uses**
- Generating boilerplate for new online/offline store providers
- Creating test suites for feature views, data sources, and transformations
- Writing docstrings and documentation
- Refactoring existing store implementations
- Generating utility functions and helper code
- Explaining existing code patterns (e.g., how point-in-time joins work)

**❌ Avoid AI For**
- Complex data correctness logic (point-in-time joins, feature retrieval) without thorough review
- Security-critical code (auth, credential handling)
- Protobuf schema changes that affect serialization compatibility
- Database migrations or schema changes
- Large architectural changes to the registry or feature server

**Workflow Tips**
- Study existing provider implementations (e.g., `sdk/python/feast/infra/online_stores/`) before generating new ones
- Build, lint, and test incrementally — run `make test-python-unit` frequently
- Always ask: "Is this correct for point-in-time semantics? Does it follow the existing provider pattern?"

**Security Considerations**
- Never expose credentials or connection strings in prompts
- Extra review required for online store connection handling, auth tokens, and user input parsing
- Follow existing patterns for credential management (environment variables, config objects)

## Testing & Review

Before submitting AI-assisted code, confirm:

- You understand every line
- All relevant tests pass locally (`make test-python-unit`)
- Integration tests pass for affected stores (`make test-python-integration-local`)
- Code follows existing provider patterns in `sdk/python/feast/`
- Protobuf changes are compiled (`make compile-protos-python`)
- Code passes linting (`make lint-python`) and type checking (`cd sdk/python && python -m mypy feast`)

Always get human review for:
- Changes to core retrieval logic (`get_online_features`, `get_historical_features`)
- New data source or store provider implementations
- Registry changes
- Protobuf schema modifications
- Anything touching point-in-time correctness

## Using Claude Code for Feast Development

- The repo has a `CLAUDE.md` at the root with project context — Claude Code loads this automatically
- Use `/plan` to structure work before implementing new providers or large features
- Protect sensitive files with `.gitignore` (e.g., `.env*`, `*.key`)
- Guide the agent by referencing existing implementations as examples in your prompts

```bash
# Install Claude Code
# https://claude.ai/code

# Start a session in the feast repo
cd feast
claude

# Use the eg-internal/skills platform skills
/plugin marketplace add eg-internal/skills
/plugin enable platform-skills

# Plan before implementing
/plan Add a new online store provider for <store-name>

# Review the plan, then implement
> plan looks good, implement
```

## Feast-Specific AI Tips

- **New provider**: Point the agent at an existing provider (e.g., `sdk/python/feast/infra/online_stores/redis.py`) and ask it to follow the same pattern
- **Tests**: Reference `sdk/python/tests/unit/` and `sdk/python/tests/integration/` for test conventions
- **Protos**: After any `.proto` change, remind the agent to run `make compile-protos-python`
- **Go changes**: If touching `go/`, remind the agent that Python and Go SDKs must stay in sync

## Community & Collaboration

- In PRs, note significant AI use and explain how you validated the output
- Contribute prompting tips and patterns back to this guide
- For repo-specific guidance, contact the owners listed in CODEOWNERS