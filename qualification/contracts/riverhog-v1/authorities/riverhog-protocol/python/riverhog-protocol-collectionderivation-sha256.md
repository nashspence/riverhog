# riverhog_protocol.CollectionDerivation.sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionderivation-sha256:b812c7e414 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a9e935f487"></a>
- <a id="s-69d4d98026"></a>`distribution`: `riverhog-protocol`
- <a id="s-9972fe003c"></a>`module`: `riverhog_protocol`
- <a id="s-b37b428df2"></a>`name`: `sha256`
- <a id="s-aaea797f6f"></a>`owner`: `riverhog_protocol.CollectionDerivation`
- <a id="s-1099225636"></a>`unit`: `member`

### Declared structure

- <a id="s-1e38d62fce"></a>`kind`: `"property"`
- <a id="s-aaf7439bd9"></a>`signature`: `"\"(self) -> 'str'\""`

## Maintained corroboration

### Related interface records

- [riverhog_protocol.CollectionDerivation](riverhog-protocol-collectionderivation.md)

## Governing policies

- <a id="pa-ce6eb311f3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionDerivation.sha256`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2cd0cf660df83fe0f9cc5e11534f78adcc26af10202730d9ef40ddc3f7e85bac -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "sha256",
  "owner": "riverhog_protocol.CollectionDerivation",
  "unit": "member"
}
```
