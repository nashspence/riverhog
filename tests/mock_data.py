from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256


def patterned_bytes(label: str, size_bytes: int) -> bytes:
    seed = sha256(label.encode("utf-8")).digest()
    repeats = (size_bytes // len(seed)) + 1
    return (seed * repeats)[:size_bytes]


@dataclass(frozen=True)
class MockFile:
    relative_path: str
    content: bytes
    mode: str = "0644"
    mtime: str = "2026-04-17T10:15:30Z"
    uid: int = 1000
    gid: int = 1000

    @property
    def size_bytes(self) -> int:
        return len(self.content)

    @property
    def sha256(self) -> str:
        return sha256(self.content).hexdigest()

    def upload_payload(self) -> dict[str, object]:
        return {
            "relative_path": self.relative_path,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "mode": self.mode,
            "mtime": self.mtime,
            "uid": self.uid,
            "gid": self.gid,
        }


def family_archive_files() -> list[MockFile]:
    return [
        MockFile(
            "home-videos/2024/summer-trip/day-01/cabin-arrival.mov",
            patterned_bytes("family-cabin-arrival", 180_000),
        ),
        MockFile(
            "home-videos/2024/summer-trip/day-02/lakeside-breakfast.mp4",
            patterned_bytes("family-lakeside-breakfast", 220_000),
        ),
        MockFile(
            "finance/2024/taxes/federal-return.pdf",
            patterned_bytes("finance-federal-return", 32_000),
        ),
    ]


def document_archive_files() -> list[MockFile]:
    return [
        MockFile(
            "financial/2025/banking/monthly-statement-jan.pdf",
            patterned_bytes("bank-statement-jan", 41_000),
        ),
        MockFile(
            "financial/2025/banking/monthly-statement-feb.pdf",
            patterned_bytes("bank-statement-feb", 39_000),
        ),
        MockFile(
            "legal/estate/trust-scan-01.pdf",
            patterned_bytes("trust-scan", 28_000),
        ),
    ]


def oversized_master_reel() -> MockFile:
    return MockFile(
        "home-videos/masters/family-archive-master-reel.mov",
        patterned_bytes("family-master-reel", 1_700_000),
    )
