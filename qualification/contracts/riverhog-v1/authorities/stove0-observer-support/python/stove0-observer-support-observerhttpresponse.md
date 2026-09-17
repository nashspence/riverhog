# stove0_observer_support.ObserverHttpResponse

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observerhttpresponse:3d5eca2d81 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-04237fe753"></a>
- <a id="s-363208f5a5"></a>`distribution`: `stove0-observer-support`
- <a id="s-c44165dd3b"></a>`module`: `stove0_observer_support`
- <a id="s-fa39a4c26e"></a>`name`: `ObserverHttpResponse`
- <a id="s-38164713be"></a>`unit`: `export`

### Declared structure

- <a id="s-e57286e561"></a>`kind`: `"class"`
- <a id="s-72fcb01819"></a>`signature`: `"\"(status: 'int', headers: 'tuple[tuple[str, str], ...]', body: 'bytes') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-398255d653"></a>`status` | `'int'` | `required` |
| <a id="s-e84e7c5e42"></a>`headers` | `'tuple[tuple[str, str], ...]'` | `required` |
| <a id="s-0aa25d3615"></a>`body` | `'bytes'` | `required` |

## Governing policies

- <a id="pa-574d18dc4e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — [reference/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ObserverHttpResponse`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1fb11e175f6290fd99bd65292ce31aebc0cca76c3cc6b91021a1b41e203d6b61 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "status",
        "type": "'int'"
      },
      {
        "default": "required",
        "name": "headers",
        "type": "'tuple[tuple[str, str], ...]'"
      },
      {
        "default": "required",
        "name": "body",
        "type": "'bytes'"
      }
    ],
    "kind": "class",
    "signature": "\"(status: 'int', headers: 'tuple[tuple[str, str], ...]', body: 'bytes') -> None\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "ObserverHttpResponse",
  "unit": "export"
}
```

</details>
