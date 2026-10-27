"""Reference-only arithmetic/admission kernel, not a Riverhog storage backend.

No SQLite calls or authority interpretation occur here. Production must implement
checked arithmetic, preallocated bookkeeping and callback lifecycle in native code.
"""
from dataclasses import dataclass, field

I64_MAX = (1 << 63) - 1


def positive(value: int) -> int:
    if type(value) is not int or not 0 < value <= I64_MAX:
        raise ValueError("positive signed-64-bit resource value required")
    return value


def ceildiv(a: int, b: int) -> int:
    return (a + b - 1) // b


def control_bound(lengths: tuple[int, ...], *, page: int = 4096,
                  sector: int = 4096, reserved: int = 0) -> tuple[int, int]:
    """Conservative fixed-BLOB write/sector-journal pages and journal extent.

    Assumes the exact fixed-length, no-topology-change profile in RESOURCE_ADMISSION.
    The journal formula is an allocation recipe, not a replacement for a hard gate.
    """
    positive(page); positive(sector)
    if page & (page - 1) or sector & (sector - 1) or not 0 <= reserved < page - 4:
        raise ValueError("unsupported page/sector geometry")
    for size in lengths:
        positive(size)
    usable = page - reserved
    frames = 1 + sum(2 + ceildiv(size, usable - 4) for size in lengths)
    pages = max(1, ceildiv(sector, page)) * frames
    extent = 2 * sector + pages * (page + 8) + 16
    if extent > I64_MAX:
        raise ValueError("resource formula overflow")
    return pages, extent


@dataclass(frozen=True)
class Limits:
    page_bytes: int
    pages: int
    database_end: int
    journal_end: int
    write_bytes: int
    calls: int

    def __post_init__(self):
        for value in vars(self).values():
            positive(value)
        if self.page_bytes & (self.page_bytes - 1):
            raise ValueError("page size must be a power of two")


class Exhausted(Exception):
    pass


@dataclass
class Gate:
    """Count attempted delegations, not just successful native return values.

    Dynamic Python sets are for this reference model only. The production design
    requires fixed-capacity tables allocated before an engine call, including an
    independent recovery bank. A rejected request must never reach the delegate.
    """
    limits: Limits
    pages: set[int] = field(default_factory=set)
    writes: int = 0
    calls: int = 0
    journal_peak: int = 0

    def write(self, kind: str, amount: int, offset: int) -> None:
        if any(type(v) is not int or v < 0 for v in (amount, offset)):
            raise Exhausted("invalid extent")
        if amount > I64_MAX - offset:
            raise Exhausted("extent overflow")
        if self.calls >= self.limits.calls or amount > self.limits.write_bytes - self.writes:
            raise Exhausted("call/byte grant")
        new: set[int] = set()
        if amount:
            end = offset + amount
            if kind == "database":
                if end > self.limits.database_end:
                    raise Exhausted("database extent grant")
                first, last = offset // self.limits.page_bytes, (end - 1) // self.limits.page_bytes
                # Do not allocate a huge range in the model on a hostile extent.
                if last - first + 1 > self.limits.pages:
                    raise Exhausted("page grant")
                new = set(range(first, last + 1)) - self.pages
                if len(new) > self.limits.pages - len(self.pages):
                    raise Exhausted("page grant")
            elif kind == "journal":
                if end > self.limits.journal_end:
                    raise Exhausted("journal extent grant")
            else:
                raise Exhausted("unadmitted file class")
        self.calls += 1
        self.writes += amount
        self.pages.update(new)
        if kind == "journal" and amount:
            self.journal_peak = max(self.journal_peak, offset + amount)


def payload_may_commit(*, backed_pages: int, end_pages: int,
                       free_pages: int, reserve_floor: int) -> bool:
    values = (backed_pages, end_pages, free_pages, reserve_floor)
    if any(type(v) is not int or v < 0 for v in values):
        return False
    return free_pages <= end_pages <= backed_pages and free_pages >= reserve_floor
