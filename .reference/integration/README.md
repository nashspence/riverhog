# Focused integration packet index

Read HANDOFF → SESSION → P00. Then select one packet and one concrete slice.
This map routes **all 64 existing G families** without declaring a single one
implemented. Dependencies are artifacts, not current issue state or new native
GitHub dependencies. Cross-cutting qualification requires every reached delegate.

```sh
python .reference/integration/read.py P00
python .reference/integration/read.py P06 --read 1
python .reference/integration/read.py --family G59
python .reference/integration/read.py --check
```

A normal packet card is under 350 words. The reader caps any excerpt at 12,000
characters, uses explicit offsets, and never recursively follows its links.
Do not read all packet cards in one session. These are routing domains, not
20 mandatory commits/issues; split a broad packet at its first focused slice.
Do not use a packet to waive ordinary review, code checks or the issue's gates.

| Packet | Focus | Predecessor artifacts |
|---|---|---|
| [P00](packets/P00.md) | Intake and initial pushback | — |
| [P01](packets/P01.md) | Executable coverage and evidence foundation | P00 |
| [P02](packets/P02.md) | Workload, conditions and approval checkpoint | P00, P01 |
| [P03](packets/P03.md) | Shared bytes, schema and resumable hash interfaces | P00, P01 |
| [P04](packets/P04.md) | Exact journal validation | P03 |
| [P05](packets/P05.md) | Corpus and selected-history access | P03, P04 |
| [P06](packets/P06.md) | Factored discovery build and Merkle sealing | P04, P05 |
| [P07](packets/P07.md) | Catalog publication, summaries and retained state | P03 |
| [P08](packets/P08.md) | Exact discovery, browsing and client progression | P06, P07 |
| [P09](packets/P09.md) | Compiled recipe and exact selection handoff | P03 |
| [P10](packets/P10.md) | Whole-scope observer input and delivery | P03, P09 |
| [P11](packets/P11.md) | Independent observation acceptance and scoped evidence | P03, P10 |
| [P12](packets/P12.md) | Fact decisions, groups and nested planning | P09, P11 |
| [P13](packets/P13.md) | Live authority, feeds and control-plane state | P01 |
| [P14](packets/P14.md) | Target control, settlement and application consumers | P03, P12, P13 |
| [P15](packets/P15.md) | Ingest, archive custody, retrieval and recovery | P03, P05 |
| [P16](packets/P16.md) | Supplied observation and filename providers | P10, P11 |
| [P17](packets/P17.md) | Other supplied components and public tooling | P01, P03, P13 |
| [P18](packets/P18.md) | Cross-owner growth and real lifecycle qualification | P01, P02 |
| [P19](packets/P19.md) | Issue closeout and downstream qualification handoff | P18 |

P00 handles initial pushback. P01 establishes the evidence path; P02 handles
required evidence-backed workload/capacity/target/cadence decisions. Subsequent
packets may prepare independently where interfaces are agreed, but dependent
activation waits for its real producer and consumer. P06/P07 and P09/P10 explicitly
coordinate shared interfaces without demanding a giant cross-owner commit.

P18/P19 are acceptance gates, not proof supplied by this reference. P13–P17
route ordinary owners one family at a time; do not turn them into speculative
new architecture studies. The governing defaults are already expressed.

Before leaving a slice, post [the checkpoint](SESSION.md#durable-checkpoint--put-this-in-github-not-tracked-main-files)
in GitHub. On a fresh context reload that checkpoint, verify source state, and
load only the selected packet. [Decisions](DECISIONS.md), [history/evidence](HISTORY.md),
and [cleanup checks](VALIDATION.md) are optional targeted references.
