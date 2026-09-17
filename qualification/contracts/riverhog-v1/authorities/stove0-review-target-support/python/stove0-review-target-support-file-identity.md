# stove0_review_target_support.file_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-file-identity:68bbe3051a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9d7e153f55"></a>
- <a id="s-b91a4d46b1"></a>`distribution`: `stove0-review-target-support`
- <a id="s-5517a7a22e"></a>`module`: `stove0_review_target_support`
- <a id="s-4510d171f0"></a>`name`: `file_identity`
- <a id="s-7b574eb671"></a>`unit`: `export`

### Declared structure

- <a id="s-0373cf9090"></a>`kind`: `"function"`
- <a id="s-375d754663"></a>`signature`: `"\"(path: 'Path') -> 'tuple[int, str]'\""`

## Governing policies

- <a id="pa-2168c985c6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — [reference/stove0/targets/review/support/src/stove0\_review\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_support.file_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f3b538e3069b3202664fccd3e8c179e09d6cc7e925b6eb51c960001599988204 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(path: 'Path') -> 'tuple[int, str]'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "file_identity",
  "unit": "export"
}
```

</details>
