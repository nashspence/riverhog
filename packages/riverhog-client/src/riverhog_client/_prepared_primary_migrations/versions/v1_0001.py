"""Create the current prepared-primary transactional byte schema."""

from alembic import op

revision: str = "v1_0001"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    from riverhog_client._prepared_primary_migrations.v1_ddl import SQLITE_DDL

    for statement in SQLITE_DDL:
        op.execute(statement)


def downgrade() -> None:
    raise RuntimeError("Prepared-primary state migrations are forward-only")
