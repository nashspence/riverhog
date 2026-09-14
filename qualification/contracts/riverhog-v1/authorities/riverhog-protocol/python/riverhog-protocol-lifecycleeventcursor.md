# riverhog_protocol.LifecycleEventCursor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-lifecycleeventcursor:8ea5b81d78 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a8af915545"></a>
- <a id="s-ef4362c490"></a>`distribution`: `riverhog-protocol`
- <a id="s-3bf565a69f"></a>`module`: `riverhog_protocol`
- <a id="s-61be6a7e47"></a>`name`: `LifecycleEventCursor`
- <a id="s-f2a762009b"></a>`unit`: `export`

### Declared structure

- <a id="s-fc50378035"></a>`kind`: `"type-alias"`
- <a id="s-8c97a084c2"></a>`value`: `"typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:0\|[1-9][0-9]*)$', ascii_only=None), AfterValidator(func=<function validate_lifecycle_event_cursor>)]"`

## Governing policies

- <a id="pa-7813bd5a10"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.LifecycleEventCursor`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e00c801e5de8e43a5d7686c41987bf0095c839536676db2111cf903ab88db96a -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "typing.Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=1, max_length=19, pattern='^(?:0|[1-9][0-9]*)$', ascii_only=None), AfterValidator(func=<function validate_lifecycle_event_cursor>)]"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "LifecycleEventCursor",
  "unit": "export"
}
```
