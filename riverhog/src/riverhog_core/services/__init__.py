"""Core application services and their source-owned validation declarations."""

from importlib import import_module as _import_module

from . import collection_uploads as _collection_uploads

# Complete each owner before binding callers that depend on its declared operations.
_import_module(".upload_journal_validation_owner", __name__)
_import_module(".upload_journal_validation_retirement", __name__)
_collection_uploads._declare_service_scheduler_growth()
