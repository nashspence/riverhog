# state_schema.attach_sha256_string_constraints

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-attach-sha256-string-constraints:d729183615 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a26c71b641"></a>
- <a id="s-87586dda81"></a>`distribution`: `state-schema`
- <a id="s-85a395d175"></a>`module`: `state_schema`
- <a id="s-f90780d970"></a>`name`: `attach_sha256_string_constraints`
- <a id="s-c93377e806"></a>`unit`: `export`

### Declared structure

- <a id="s-cfcb6a99a9"></a>`kind`: `"function"`
- <a id="s-f3921c9659"></a>`signature`: `"\"(metadata: 'MetaData') -> 'None'\""`

## Governing policies

- <a id="pa-d18da47d5f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.attach_sha256_string_constraints`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 23e285602bead2f1fa4708f307a148b95a576b12391cc6fc309ded4c64b126dd -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(metadata: 'MetaData') -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "attach_sha256_string_constraints",
  "unit": "export"
}
```
