# riverhog_protocol.ProcessingClaimRestartDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimrestartdocument-get:fdba9eb07d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2093bd227a"></a>
- <a id="s-ab67070daa"></a>`distribution`: `riverhog-protocol`
- <a id="s-560012c18e"></a>`module`: `riverhog_protocol`
- <a id="s-353843bbfe"></a>`name`: `get`
- <a id="s-3c38276bb8"></a>`owner`: `riverhog_protocol.ProcessingClaimRestartDocument`
- <a id="s-d36d0e5dec"></a>`unit`: `member`

### Declared structure

- <a id="s-df9c1fbeb6"></a>`kind`: `"method"`
- <a id="s-eb901f2ab7"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingClaimRestartDocument](riverhog-protocol-processingclaimrestartdocument.md)

## Governing policies

- <a id="pa-bc3543aac0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimRestartDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 35cf84f3da9b5e8473a23a7c093851da5a5bce2a91fd518e3c727e544ae46900 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.ProcessingClaimRestartDocument",
  "unit": "member"
}
```

</details>
