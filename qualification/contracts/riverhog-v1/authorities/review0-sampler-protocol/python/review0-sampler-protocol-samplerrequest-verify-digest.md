# review0_sampler_protocol.SamplerRequest.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerrequest-v-02daf55cd1:7fdb510d94 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2b336b008f"></a>
- <a id="s-8a07c506c9"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-33361aea65"></a>`module`: `review0_sampler_protocol`
- <a id="s-4fa79bae0d"></a>`name`: `verify_digest`
- <a id="s-a08ad5b78c"></a>`owner`: `review0_sampler_protocol.SamplerRequest`
- <a id="s-f96fb4b8ed"></a>`unit`: `member`

### Declared structure

- <a id="s-6bb2434391"></a>`kind`: `"method"`
- <a id="s-749ffcb5d1"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [SamplerRequest](review0-sampler-protocol-samplerrequest.md)

## Governing policies

- <a id="pa-e16fd2947c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerRequest.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 88407b3a1f28802584febe7077bbcb7aa77fafdf95083d894f43951023055918 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "verify_digest",
  "owner": "review0_sampler_protocol.SamplerRequest",
  "unit": "member"
}
```

</details>
