# stove0_exiftool_observer.ExiftoolObserver

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-exiftool-observer:stove0-exiftool-observer-exiftoolobserver:14ebf73ea3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-exiftool-observer](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-af263598fd"></a>
- <a id="s-dec08ca6a2"></a>`distribution`: `stove0-exiftool-observer`
- <a id="s-cac672472d"></a>`module`: `stove0_exiftool_observer`
- <a id="s-ecc5d68877"></a>`name`: `ExiftoolObserver`
- <a id="s-ad6bf94f09"></a>`unit`: `export`

### Declared structure

- <a id="s-26c896080f"></a>`kind`: `"class"`
- <a id="s-facbfcdd8a"></a>`signature`: `"\"(*, exiftool: 'str' = 'exiftool', workspace_root: 'Path \| None' = None, source_revision: 'str' = 'unknown', image_digest: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [execution_evidence](stove0-exiftool-observer-exiftoolobserver-execution-evidence.md)
- [descriptor](stove0-exiftool-observer-exiftoolobserver-descriptor.md)
- [observe](stove0-exiftool-observer-exiftoolobserver-observe.md)

## Governing policies

- <a id="pa-228af01e87"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-exiftool-observer:stove0_exiftool_observer](../../../evidence/sources.md#src-18b5d27762) — `reference/stove0/observers/exiftool/src/stove0_exiftool_observer/__init__.py`

### Machine authority

- `/external_contract/python/stove0_exiftool_observer.ExiftoolObserver`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8ef6c51a40e78a4da0329f4103959fb5e85bbbec75c977180bf901fd65cb50ee -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(*, exiftool: 'str' = 'exiftool', workspace_root: 'Path | None' = None, source_revision: 'str' = 'unknown', image_digest: 'str') -> 'None'\""
  },
  "distribution": "stove0-exiftool-observer",
  "module": "stove0_exiftool_observer",
  "name": "ExiftoolObserver",
  "unit": "export"
}
```

</details>
