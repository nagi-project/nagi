"""Generate Markdown attribute tables from JSON Schema files.

Usage:
    python scripts/gen_schema_docs.py docs/schemas docs/configurations/resources
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Map schema file names to (relative doc path from docs_root, page title, table_only)
# table_only=True suppresses ## Attributes heading (for embedding in existing sections)
SCHEMA_MAP = {
    "Metadata": ("reference/resources/index.md", "metadata", False),
    "AssetSpec": ("reference/resources/asset.md", "kind: Asset", False),
    "OnDriftEntry": ("reference/resources/asset.md", "OnDriftEntry", True),
    "ConnectionSpec": ("reference/resources/connection.md", "kind: Connection", False),
    "SyncSpec": ("reference/resources/sync.md", "kind: Sync", False),
    "SyncStep": ("reference/resources/sync.md", "SyncStep", True),
    "ConditionsSpec": ("reference/resources/conditions.md", "kind: Conditions", False),
    "OriginSpec": ("reference/resources/origin.md", "kind: Origin", False),
    "NagiConfig": ("reference/project.md", "nagi.yaml", False),
    "AssetEvalResult": ("architecture/storage.md", "Cache", True),
    "LockInfo": ("architecture/storage.md", "Locks", True),
    "SuspendedInfo": ("architecture/storage.md", "Suspended", True),
    "SyncLogEntry": ("architecture/storage.md", "Logs", True),
}


def resolve_ref(ref: str, definitions: dict) -> dict:
    name = ref.split("/")[-1]
    return definitions.get(name, {})


def schema_to_type(prop: dict, definitions: dict) -> str:
    if "$ref" in prop:
        resolved = resolve_ref(prop["$ref"], definitions)
        fmt = resolved.get("format", "")
        if fmt == "duration":
            return "Duration"
        if fmt == "cron":
            return "CronSchedule"
        return prop["$ref"].split("/")[-1]

    if "anyOf" in prop:
        types = []
        for variant in prop["anyOf"]:
            if variant.get("type") == "null":
                continue
            types.append(schema_to_type(variant, definitions))
        return " | ".join(types) if types else "any"

    if "oneOf" in prop:
        return "oneOf (see below)"

    t = prop.get("type", "any")
    if isinstance(t, list):
        t = [x for x in t if x != "null"]
        return t[0] if len(t) == 1 else " | ".join(t)

    if t == "array":
        items = prop.get("items", {})
        item_type = schema_to_type(items, definitions)
        return f"list[{item_type}]"

    if t == "object":
        additional = prop.get("additionalProperties", {})
        if additional:
            val_type = schema_to_type(additional, definitions)
            return f"map[string, {val_type}]"
        return "object"

    return t


def is_required(name: str, required: list[str]) -> str:
    return "Yes" if name in required else "—"


def is_nullable(prop: dict) -> bool:
    if "anyOf" in prop:
        return any(v.get("type") == "null" for v in prop["anyOf"])
    t = prop.get("type", "")
    if isinstance(t, list):
        return "null" in t
    return False


def get_default(prop: dict) -> str:
    if "default" in prop:
        val = prop["default"]
        if val is None:
            return ""
        if isinstance(val, bool):
            return str(val).lower()
        if isinstance(val, list):
            return "[]"
        return str(val)
    return ""


def render_properties_table(
    properties: dict, required: list[str], definitions: dict, prefix: str = ""
) -> list[str]:
    lines = []
    lines.append("| Attribute | Type | Required | Default | Description |")
    lines.append("| --- | --- | --- | --- | --- |")

    def _add_props(props: dict, req_list: list[str], pfx: str) -> None:
        sorted_names = sorted(props.keys(), key=lambda n: (n not in req_list, n))
        for name in sorted_names:
            prop = props[name]
            full_name = f"{pfx}{name}" if pfx else name

            # If the property is a $ref to an object, flatten its fields
            ref_to_resolve = None
            is_optional_ref = False
            if "$ref" in prop:
                ref_to_resolve = prop["$ref"]
            elif "anyOf" in prop:
                # Option<T> generates anyOf with a $ref and null
                for v in prop["anyOf"]:
                    if "$ref" in v:
                        ref_to_resolve = v["$ref"]
                        is_optional_ref = True
                        break

            if ref_to_resolve:
                resolved = resolve_ref(ref_to_resolve, definitions)
                # Only flatten required $ref objects (not Option<T>)
                if (
                    not is_optional_ref
                    and resolved.get("type") == "object"
                    and "properties" in resolved
                ):
                    nested_req = resolved.get("required", [])
                    _add_props(
                        resolved["properties"], nested_req, f"{full_name}."
                    )
                    continue

            type_str = schema_to_type(prop, definitions)
            req = is_required(name, req_list)
            default = get_default(prop) or "-"
            desc = (prop.get("description", "") or "-").replace("\n", " ")
            lines.append(f"| `{full_name}` | {type_str} | {req} | {default} | {desc} |")

    _add_props(properties, required, prefix)
    return lines


# Display names for type discriminator values.
# Used in oneOf variant headings to show the canonical product name.
TYPE_DISPLAY_NAMES: dict[str, str] = {
    "bigquery": "BigQuery",
    "duckdb": "DuckDB",
    "snowflake": "Snowflake",
}


def render_oneof_variants(
    variants: list[dict], definitions: dict
) -> list[str]:
    lines = []
    for variant in variants:
        props = variant.get("properties", {})
        required = variant.get("required", [])
        desc = variant.get("description", "")
        type_field = props.get("type", {})
        type_name = ""
        if "const" in type_field:
            type_name = type_field["const"]
        elif "enum" in type_field:
            type_name = type_field["enum"][0]

        if type_name:
            display = TYPE_DISPLAY_NAMES.get(type_name, type_name)
            lines.append(f"### type: {display}")
            lines.append("")
        if desc:
            lines.append(desc.replace("\n", " "))
            lines.append("")

        # Filter out the 'type' discriminator field
        filtered_props = {k: v for k, v in props.items() if k != "type"}
        filtered_required = [r for r in required if r != "type"]
        lines.extend(render_properties_table(filtered_props, filtered_required, definitions))
        lines.append("")
    return lines


def _find_referring_field(
    defn_name: str, properties: dict, definitions: dict
) -> str | None:
    """Find the field name in properties that references a given definition (directly or via items)."""
    ref_suffixes = (f"#/$defs/{defn_name}", f"#/definitions/{defn_name}")
    for field_name, prop in properties.items():
        # Direct $ref
        if prop.get("$ref") in ref_suffixes:
            return field_name
        # Array whose items reference the definition
        items = prop.get("items", {})
        if items.get("$ref") in ref_suffixes:
            return field_name
        # anyOf containing the ref
        for variant in prop.get("anyOf", []):
            if variant.get("$ref") in ref_suffixes:
                return field_name
            if variant.get("items", {}).get("$ref") in ref_suffixes:
                return field_name
    # Check if any intermediate definition references it (e.g., DesiredSetEntry -> DesiredCondition)
    for other_name, other_defn in definitions.items():
        if other_name == defn_name:
            continue
        for variant in other_defn.get("anyOf", []):
            if variant.get("$ref") in ref_suffixes:
                # Found: other_name references defn_name. Now find who references other_name.
                parent = _find_referring_field(other_name, properties, definitions)
                if parent:
                    return parent
    return None


def render_schema(schema: dict, table_only: bool = False) -> list[str]:
    definitions = schema.get("$defs", schema.get("definitions", {}))
    lines = []

    # Top-level object
    if schema.get("type") == "object":
        props = schema.get("properties", {})
        required = schema.get("required", [])
        if not table_only:
            lines.append("## Attributes")
            lines.append("")
        lines.extend(render_properties_table(props, required, definitions))
        lines.append("")

        # Render nested oneOf types (e.g., DesiredCondition variants)
        if not table_only:
            for defn_name, defn in definitions.items():
                if "oneOf" in defn:
                    section = _find_referring_field(defn_name, props, definitions) or defn_name
                    lines.append(f"## {section}")
                    lines.append("")
                    desc = defn.get("description", "")
                    if desc:
                        lines.append(desc.replace("\n", " "))
                        lines.append("")
                    lines.extend(render_oneof_variants(defn["oneOf"], definitions))

    # Top-level oneOf (e.g., OriginSpec)
    elif "oneOf" in schema:
        if not table_only:
            lines.append("## Attributes")
            lines.append("")
        lines.extend(render_oneof_variants(schema["oneOf"], definitions))

    # Newtype wrapper (e.g., DesiredGroupSpec wrapping a list)
    elif "items" in schema or schema.get("type") == "array":
        items = schema.get("items", {})
        if "oneOf" in items:
            if not table_only:
                lines.append("## Attributes")
                lines.append("")
            lines.extend(render_oneof_variants(items["oneOf"], definitions))
        elif "$ref" in items:
            resolved = resolve_ref(items["$ref"], definitions)
            if "oneOf" in resolved:
                if not table_only:
                    lines.append("## Attributes")
                    lines.append("")
                lines.extend(render_oneof_variants(resolved["oneOf"], definitions))

    return lines


def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <schemas_dir> <docs_src_dir>")
        sys.exit(1)

    schemas_dir = Path(sys.argv[1])
    docs_src_dir = Path(sys.argv[2])

    for schema_name, (doc_path, title, table_only) in SCHEMA_MAP.items():
        schema_path = schemas_dir / f"{schema_name}.json"
        if not schema_path.exists():
            print(f"warning: {schema_path} not found, skipping")
            continue

        schema = json.loads(schema_path.read_text())
        lines = render_schema(schema, table_only=table_only)

        output_path = docs_src_dir / doc_path
        # Read existing file to preserve hand-written content before "## Attributes"
        existing = ""
        if output_path.exists():
            existing = output_path.read_text()

        # Replace content between start and end markers (supports per-schema markers)
        start_marker = f"<!-- schema:auto-generated:start:{schema_name} -->"
        end_marker = f"<!-- schema:auto-generated:end:{schema_name} -->"
        # Remove trailing empty lines from generated content
        while lines and lines[-1] == "":
            lines.pop()
        generated = "\n".join(lines) + "\n"

        if start_marker in existing and end_marker in existing:
            before = existing[: existing.index(start_marker)]
            after = existing[existing.index(end_marker) + len(end_marker) :]
            content = before + start_marker + "\n\n" + generated + "\n" + end_marker + after
        elif start_marker in existing:
            before = existing[: existing.index(start_marker)]
            content = before + start_marker + "\n\n" + generated + "\n" + end_marker + "\n"
        else:
            content = (
                existing.rstrip()
                + "\n\n"
                + start_marker
                + "\n\n"
                + generated
                + "\n"
                + end_marker
                + "\n"
            )

        output_path.write_text(content)
        print(f"  wrote {output_path}")


if __name__ == "__main__":
    main()
