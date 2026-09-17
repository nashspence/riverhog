# riverhog_protocol.ArtifactDisposition.from_mapping

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-artifactdisposition-from-mapping:61103b525f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2bc258bb2c"></a>
- <a id="s-c65daf0afa"></a>`distribution`: `riverhog-protocol`
- <a id="s-707521bd8c"></a>`module`: `riverhog_protocol`
- <a id="s-8c7dffb4f3"></a>`name`: `from_mapping`
- <a id="s-99848a40c8"></a>`owner`: `riverhog_protocol.ArtifactDisposition`
- <a id="s-9b6adfca63"></a>`unit`: `member`

### Declared structure

- <a id="s-725dfee476"></a>`kind`: `"classmethod"`
- <a id="s-b18d24b81f"></a>`signature`: `"\"(cls, value: 'Mapping[str, object]') -> 'ArtifactDisposition'\""`

## Maintained corroboration

### Related interface records

- [ArtifactDisposition](riverhog-protocol-artifactdisposition.md)

## Governing policies

- <a id="pa-ff5639360d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.ArtifactDisposition.from_mapping`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 57d2040294a2e08727a65cced165feafe09d2e828639a80f81c0fe2abc61684f -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'Mapping[str, object]') -> 'ArtifactDisposition'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "from_mapping",
  "owner": "riverhog_protocol.ArtifactDisposition",
  "unit": "member"
}
```

</details>
