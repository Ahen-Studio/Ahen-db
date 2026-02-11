"""
Schema definition for Skypydb database tables.
This file defines all tables, their columns, types, and indexes.
"""

from skypydb.schema import defineSchema, defineTable
from skypydb.schema.values import value

# Define the schema with all tables
schema = defineSchema({
    
    # Table for success logs
    "success": defineTable({
        "component": value.string(),
        "action": value.string(),
        "message": value.string(),
        "details": value.optional(value.string()),
        "user_id": value.optional(value.string()),
    })
    .index("by_component", ["component"])
    .index("by_action", ["action"])
    .index("by_user", ["user_id"])
    .index("by_component_and_action", ["component", "action"]),

    # Table for warning logs
    "warning": defineTable({
        "component": value.string(),
        "action": value.string(),
        "message": value.string(),
        "details": value.optional(value.string()),
        "user_id": value.optional(value.string()),
    })
    .index("by_component", ["component"])
    .index("by_action", ["action"])
    .index("by_user", ["user_id"])
    .index("by_component_and_action", ["component", "action"]),

    # Table for error logs
    "error": defineTable({
        "component": value.string(),
        "action": value.string(),
        "message": value.string(),
        "details": value.optional(value.string()),
        "user_id": value.optional(value.string()),
    })
    .index("by_component", ["component"])
    .index("by_action", ["action"])
    .index("by_user", ["user_id"])
    .index("by_component_and_action", ["component", "action"]),
})
