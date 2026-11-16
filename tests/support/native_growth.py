"""Record native counter totals alongside independently checked per-call samples."""


def record_native_growth(record_property, samples):
    record_property("growth_samples", samples)
    record_property(
        "growth_observations",
        {
            "operations": len(samples),
            **{
                metric: sum(sample["metrics"][metric] for sample in samples)
                for metric in (
                    "open_calls",
                    "dup_calls",
                    "fstat_calls",
                    "close_calls",
                    "control_octets",
                )
            },
        },
    )
