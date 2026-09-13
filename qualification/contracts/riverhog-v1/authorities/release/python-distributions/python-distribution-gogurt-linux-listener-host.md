# Python distribution: gogurt-linux-listener-host

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python-distributions:release:python-distribution-gogurt-linux-listener-host:ef3f69d0d4 -->

Optional nonnormative Linux systemd-user listener-host reference for Gogurt.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Python Distributions](index.md) |

## External contract

<a id="s-83936a7804"></a>
| Concern | Contract |
|---|---|
| <a id="s-5fd75f8ffd"></a>`artifacts` | [{"coordinate": "dist/gogurt_linux_listener_host-{version}-py3-none-any.whl", "format": "wheel"}, {"coordinate": "dist/gogurt_linux_listener_host-{version}.tar.gz", "format": "sdist"}] |
| <a id="s-b3cf83c6b2"></a>`channel` | github-release |
| <a id="s-1c10b36f4a"></a>`description` | Optional nonnormative Linux systemd-user listener-host reference for Gogurt. |
| <a id="s-32ac65e075"></a>`requires_python` | >=3.12 |
| <a id="s-87a0da3533"></a>`role` | reference_component |
| <a id="s-f9fd72a63e"></a>`source` | reference/gogurt/listener-host/linux/pyproject.toml |

## Existing ownership context

Publication preserves these existing component authorities; it does not reclassify or duplicate their interfaces.

- [gogurt-linux-listener-host](../../../evidence/relationships.md#rn-81398abad2)

## Governing policies

- <a id="pa-796344b92e"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-0db1c61aa6"></a>[publication/role-retention/v1](../../../policies/index.md#p-3e4dc2e851)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-distribution:gogurt-linux-listener-host](../../../evidence/sources.md#src-388e655d12) — `reference/gogurt/listener-host/linux/pyproject.toml`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/distributions/gogurt-linux-listener-host`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6953a9918524d73807b5ae66cf628776bea1b6aff38a14b420d5a47ab4bbcfd0 -->

```json
{
  "artifacts": [
    {
      "coordinate": "dist/gogurt_linux_listener_host-{version}-py3-none-any.whl",
      "format": "wheel"
    },
    {
      "coordinate": "dist/gogurt_linux_listener_host-{version}.tar.gz",
      "format": "sdist"
    }
  ],
  "channel": "github-release",
  "description": "Optional nonnormative Linux systemd-user listener-host reference for Gogurt.",
  "requires_python": ">=3.12",
  "role": "reference_component",
  "source": "reference/gogurt/listener-host/linux/pyproject.toml"
}
```
