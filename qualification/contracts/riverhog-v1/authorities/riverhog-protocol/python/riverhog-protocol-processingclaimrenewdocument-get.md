# riverhog_protocol.ProcessingClaimRenewDocument.get

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-processingclaimrenewdocument-get:1d365686be -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7458a60be6"></a>
- <a id="s-7b1fe2f5e0"></a>`distribution`: `riverhog-protocol`
- <a id="s-1abe14b0f1"></a>`module`: `riverhog_protocol`
- <a id="s-c65f2896ae"></a>`name`: `get`
- <a id="s-7dcfd5fdb8"></a>`owner`: `riverhog_protocol.ProcessingClaimRenewDocument`
- <a id="s-825303a389"></a>`unit`: `member`

### Declared structure

- <a id="s-965cd97f16"></a>`kind`: `"method"`
- <a id="s-e99a7d90f1"></a>`signature`: `"\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [ProcessingClaimRenewDocument](riverhog-protocol-processingclaimrenewdocument.md)

## Governing policies

- <a id="pa-acc22e60bd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.ProcessingClaimRenewDocument.get`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e6f8741f9c6c53172348d9c83d011a5352c627891509addb2a7c9d85ba3880f6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, key: 'str', default: 'Any' = None) -> 'Any'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "get",
  "owner": "riverhog_protocol.ProcessingClaimRenewDocument",
  "unit": "member"
}
```

</details>
