# riverhog_storage_adapter_protocol.ObjectReadStream.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-objectr-b7bd71463c:25ccca1269 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c80c416e85"></a>
- <a id="s-088a065ef6"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-49c7d96a06"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-7bdb8cce6b"></a>`name`: `close`
- <a id="s-43c0e4e02d"></a>`owner`: `riverhog_storage_adapter_protocol.ObjectReadStream`
- <a id="s-468431478d"></a>`unit`: `member`

### Declared structure

- <a id="s-a41919c041"></a>`kind`: `"method"`
- <a id="s-acb57120b3"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ObjectReadStream](riverhog-storage-adapter-protocol-objectreadstream.md)

## Governing policies

- <a id="pa-b6513f3ab3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.ObjectReadStream.close`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c34b4dba53dd9a4bbe01b544e9932755ecd0deb843cb5806ced0ad0da088739f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "close",
  "owner": "riverhog_storage_adapter_protocol.ObjectReadStream",
  "unit": "member"
}
```
