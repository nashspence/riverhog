# stove0_protocol.ControllerEvidence.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-controllerevidence-seal:e460704e6f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-319e15f5a6"></a>
- <a id="s-5c62f0f9f4"></a>`distribution`: `stove0-protocol`
- <a id="s-62e753962e"></a>`module`: `stove0_protocol`
- <a id="s-9c20edfcb2"></a>`name`: `seal`
- <a id="s-c691bca7f0"></a>`owner`: `stove0_protocol.ControllerEvidence`
- <a id="s-5a902004ed"></a>`unit`: `member`

### Declared structure

- <a id="s-14320ee81c"></a>`kind`: `"classmethod"`
- <a id="s-7df5e6651e"></a>`signature`: `"\"(cls, payload: 'ControllerEvidencePayload') -> 'ControllerEvidence'\""`

## Maintained corroboration

### Related interface records

- [ControllerEvidence](stove0-protocol-controllerevidence.md)

## Governing policies

- <a id="pa-ace226a667"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ControllerEvidence.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e3e4dc3df9f5ebafa1c153ad5f1fd6646570722cdf5938e92cbd730bd46deb15 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'ControllerEvidencePayload') -> 'ControllerEvidence'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "seal",
  "owner": "stove0_protocol.ControllerEvidence",
  "unit": "member"
}
```

</details>
