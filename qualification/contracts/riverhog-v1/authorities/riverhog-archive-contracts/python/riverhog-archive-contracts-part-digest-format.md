# riverhog_archive_contracts.PART_DIGEST_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-part-digest-format:865688e3c7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5aa9b7615f"></a>
| Field | Shape |
|---|---|
| <a id="s-6949caf98b"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-6614af84fe"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-e5d9e2d9a9"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-daa08ed430"></a>`name` | "PART_DIGEST_FORMAT" |
| <a id="s-5bc4e22471"></a>`unit` | "export" |

## Governing policies

- <a id="pa-097abc4075"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.PART_DIGEST_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 98fd9c1383cee442d683f3e8906d258b07ade1014b640bf22dd2480a794297f5 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "sha256"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "PART_DIGEST_FORMAT",
  "unit": "export"
}
```
