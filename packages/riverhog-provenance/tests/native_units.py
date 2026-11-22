"""Explicit one-unit budgets for tests addressing a private Native epoch."""

from dataclasses import replace

from riverhog_provenance.journal_session import (
    JournalValidationBudget,
    JournalValidationWorkBudget,
)


def unit_budget(**values):
    work = values.pop("work", JournalValidationWorkBudget())
    return JournalValidationBudget(**values, work=replace(work, transitions=1))
