"""Database schema inspector package.

This package provides a flexible framework for inspecting database schemas
across different database types. It includes:

- Base inspector class defining the common interface
- Database-specific implementations
- Factory for creating appropriate inspectors
"""

from .base import BaseSchemaInspector
from .snowflake import SnowflakeInspector
from .factory import InspectorFactory

__all__ = [
    'BaseSchemaInspector',
    'SnowflakeInspector',
    'InspectorFactory',
]

# Initialize factory with built-in inspectors
InspectorFactory.register_inspector('snowflake', SnowflakeInspector)
