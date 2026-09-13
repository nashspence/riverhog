# riverhog_archive_contracts.SELECTIVE_READ_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-archive-contracts:riverhog-archive-contracts-selective-read-format:edc9366367 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5a13ef5056"></a>
| Field | Shape |
|---|---|
| <a id="s-9d13898d98"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-645c3fa731"></a>`distribution` | "riverhog-archive-contracts" |
| <a id="s-b451282d4b"></a>`module` | "riverhog_archive_contracts" |
| <a id="s-42554b2af3"></a>`name` | "SELECTIVE_READ_FORMAT" |
| <a id="s-93c830192c"></a>`unit` | "export" |

## Governing policies

- <a id="pa-2a87dac590"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-archive-contracts:riverhog_archive_contracts](../../../evidence/sources.md#src-4557222ddc) — `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_archive_contracts.SELECTIVE_READ_FORMAT`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1eebdde5f9fd67d6174fffc864cea21c681a1c8f8b48338f3273531ca17d3410 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "age-chunk-range/v1"
  },
  "distribution": "riverhog-archive-contracts",
  "module": "riverhog_archive_contracts",
  "name": "SELECTIVE_READ_FORMAT",
  "unit": "export"
}
```
