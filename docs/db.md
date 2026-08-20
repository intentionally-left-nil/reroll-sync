The sqlite database will have the following tables:

* `pypi_index` - stores the data about each pypi project
* `wheels` - stores the data about each filename
* `errors` - stores error data when parsing wheels (can be truncated if desired)

# pypi_index table
* `name` (primary_key) - The name of the python package
* `serial` - The pypi serial (higher indicating newer data than before)
* `updated_at` - The time the entire index was refreshed, not row specific

# Wheels
* `filename` (unique, indexed) - the actual name of the wheel, not normalized, depending on pypi to keep globally unique
* `project` (indexed) - the project the wheel refers to
* `pypi_simple` - The JSON from the simple/projects API corresponding to the wheel
* `skip_reason` - a structured string as to why not to partially or fully parse the wheel
* `metadata_downloaded_at` - The time the whl.metadata file was downloaded, NULL if not existing
* `wheel_metadata` - The parsed & cleaned data from a METADATA file
* `metadata_reroll_version` - The version that reroll used to parse the metadata field
* `repodata` - The JSON containing zero-or-more repodata entries for the wheel
* `repodata_reroll_version` The version that reroll used to parse the repodata
* `updated_at` - The time the row was created or updated

# Errors
* `wheel_filename` - Foreign key to the wheel.filename
* `error_category` - A structured name of the type of error encountered
* `error_subcategory` - A structured name of a detailed error type
* `details` - Freeform text of the data
* `reroll_version` - The reroll version that encountered the error
* `created_at` - The time corresponding to the updated_at time in the original wheel table
