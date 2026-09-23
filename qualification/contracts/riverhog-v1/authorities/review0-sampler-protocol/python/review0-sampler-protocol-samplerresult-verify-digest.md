# review0_sampler_protocol.SamplerResult.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerresult-verify-digest:710190dce7 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-614a7e87a4"></a>
- <a id="s-e7ad8a47d5"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-bf58de77b6"></a>`module`: `review0_sampler_protocol`
- <a id="s-95f87bda46"></a>`name`: `verify_digest`
- <a id="s-fa29e18a93"></a>`owner`: `review0_sampler_protocol.SamplerResult`
- <a id="s-b07e64647a"></a>`unit`: `member`

### Declared structure

- <a id="s-44c5696b38"></a>`kind`: `"method"`
- <a id="s-9287d3ff62"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [SamplerResult](review0-sampler-protocol-samplerresult.md)

## Governing policies

- <a id="pa-6bca0e1593"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerResult.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61ec69dad4c3ba261059f88b36a107e116608a436bc0a8b1b2002454d4478334 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "verify_digest",
  "owner": "review0_sampler_protocol.SamplerResult",
  "unit": "member"
}
```

</details>
