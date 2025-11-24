"""CloudVision table schema resource."""

from schema_docs import get_table_schema


def get_cloudvision_schema_resource_impl() -> str:
    """Implementation for CloudVision schema resource."""
    schema = get_table_schema("cloudvision")
    return f"""# GDELT CloudVision Table Schema

**Table:** {schema['table_name']}

**Description:** {schema['description']}

## Fields

{chr(10).join([f"- **{field['name']}** ({field['type']}): {field['description']}" for field in schema['fields']])}

## Sample Queries

{chr(10).join([f"### {q['description']}{chr(10)}```sql{chr(10)}{q['query']}{chr(10)}```{chr(10)}" for q in schema['sample_queries']])}
"""
