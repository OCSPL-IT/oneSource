# db_router.py

class ReadOnlyDBRouter:
    """
    A database router that directs read operations for specific models and apps
    to their respective databases, while preventing writes and migrations on them.
    """

    def _is_bom_model(self, model):
        """
        Helper function to identify the new BOM models, which live in the
        'daily_checks' app but belong to the 'production_scheduler' database.
        """
        return (
            model._meta.app_label == "daily_checks"
            and model._meta.model_name.lower() in ["bomheader", "bomline", "bomequipment"]
        )

    # 🔴 NEW: special case for AuditLog
    def _is_audit_model(self, model):
        return (
            model._meta.app_label == "main"
            and model._meta.model_name.lower() == "auditlog"
        )

    def db_for_read(self, model, **hints):
        """
        Directs read operations to the correct database.
        The new BOM models are checked first.
        """
        # --- START: NEW LOGIC ---
        if self._is_bom_model(model):
            return "production_scheduler"

        # AuditLog must live in default DB (NOT readonly_db)
        if self._is_audit_model(model):
            return "default"
        # --- END: NEW LOGIC ---

        # Your existing rules
        if model._meta.app_label == "main":
            return "readonly_db"
        elif model._meta.app_label == "scheduler":
            return "production_scheduler"
        elif model._meta.app_label == "contract_database":
            return "contract_database"

        # For any other model, use the default database
        return "default"

    def db_for_write(self, model, **hints):
        """
        Prevents writing to any of the specified external databases.
        Writes for other apps will fall back to the 'default' database.
        """
        # --- START: NEW LOGIC ---
        if self._is_bom_model(model):
            return None  # Prevent writing

        # Allow writes for AuditLog on default DB
        if self._is_audit_model(model):
            return "default"
        # --- END: NEW LOGIC ---

        # Your existing apps that should be read-only
        if model._meta.app_label in ["main", "scheduler", "contract_database"]:
            return None  # Prevent writing

        return "default"

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations if both models are part of the BOM group.
        """
        if self._is_bom_model(obj1) and self._is_bom_model(obj2):
            return True

        # You can add similar logic for your other apps if needed
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Prevents Django from running migrations on the external databases.
        """
        # Prevent migrations for the new BOM models specifically
        if app_label == "daily_checks" and model_name in [
            "bomheader",
            "bomline",
            "bomequipment",
        ]:
            return False

        # Your existing rule covers the databases by name
        if db in ["readonly_db", "production_scheduler", "contract_database"]:
            return False

        # (Optional but explicit) – allow AuditLog on default
        if db == "default" and app_label == "main" and model_name == "auditlog":
            return True

        return True
