# riverhog-upload-work-progression/v1

[Atlas](../../../index.md) · [Reference navigation](../index.md)

<a id="e-5707b3a2d3-b068c34cbb"></a>
Recorded owner: `riverhog`.

- [Affected contract elements (1)](contracts.md)
- [Candidate tests and reviewed scopes (3)](tests.md)

## Guarantees still needing evidence

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Recorded open-claim keys: `bounded_step`, `forward_progress`, `multiple_segments`, `no_silent_truncation`, `restart`.

## Governing rules

- [extent-rule/bounded-segment/v1](../../../authorities/extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)

Recorded reasons: `bounded-actionable-work-acquisition`.

The policy definitions explain the contract. Candidate tests describe potential evidence; their presence does not establish these group-wide guarantees.
