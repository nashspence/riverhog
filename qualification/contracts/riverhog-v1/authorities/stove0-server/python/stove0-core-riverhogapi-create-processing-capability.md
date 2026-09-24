# stove0_core.RiverhogApi.create_processing_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-create-processing-capability:b57860c900 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ce98bf8004"></a>
- <a id="s-d2d3137c22"></a>`distribution`: `stove0-server`
- <a id="s-5b5511b560"></a>`module`: `stove0_core`
- <a id="s-9157b79927"></a>`name`: `create_processing_capability`
- <a id="s-e925b2c6ea"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-6a24d93482"></a>`unit`: `member`

### Declared structure

- <a id="s-857cb356ab"></a>`kind`: `"method"`
- <a id="s-e4a494bd9f"></a>`signature`: `"\"(self, claim_id: 'str', *, fence: 'int', audience: 'str', actions: 'Sequence[CapabilityAction]' = ('read-inputs',), artifacts: 'Iterable[Mapping[str, Any]]', ttl_seconds: 'int' = 900) -> 'ProcessingCapabilityDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-1fff9b3ead"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.create_processing_capability`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e46801af345b3eda967d194997a382e85918564a95320310652e2dbfa7d9cced -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, claim_id: 'str', *, fence: 'int', audience: 'str', actions: 'Sequence[CapabilityAction]' = ('read-inputs',), artifacts: 'Iterable[Mapping[str, Any]]', ttl_seconds: 'int' = 900) -> 'ProcessingCapabilityDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "create_processing_capability",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
