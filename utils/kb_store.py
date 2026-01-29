"""
File-system based Knowledge Base store.
Manages schema, prompt construction, decomposition, and retrieval per knowledge base.
"""

import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from utils.logger import logger

# Root directory for KB storage (file-system "database")
KB_STORE_ROOT = "kb_store"
INDEX_FILE = "index.json"
META_FILE = "meta.json"
SCHEMA_FILE = "schema.json"
PROMPTS_FILE = "prompts.json"


def _store_dir() -> Path:
    """Return the root path for KB store."""
    return Path(KB_STORE_ROOT)


def _kb_dir(kb_id: str) -> Path:
    """Return the directory path for a single KB."""
    return _store_dir() / kb_id


def _ensure_store() -> None:
    """Ensure root store directory exists."""
    _store_dir().mkdir(parents=True, exist_ok=True)


def _load_index() -> List[Dict[str, Any]]:
    """Load the index of all knowledge bases."""
    _ensure_store()
    path = _store_dir() / INDEX_FILE
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.warning(f"Failed to load KB index: {e}")
        return []


def _save_index(entries: List[Dict[str, Any]]) -> None:
    """Save the index file."""
    _ensure_store()
    path = _store_dir() / INDEX_FILE
    with open(path, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)


def list_knowledge_bases() -> List[Dict[str, Any]]:
    """
    List all knowledge bases.
    Returns list of { id, name, dataset_name, created_at, updated_at }.
    """
    index = _load_index()
    result = []
    for entry in index:
        kb_id = entry.get("id")
        if not kb_id:
            continue
        meta_path = _kb_dir(kb_id) / META_FILE
        if meta_path.exists():
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                result.append({
                    "id": kb_id,
                    "name": meta.get("name", kb_id),
                    "dataset_name": meta.get("dataset_name", ""),
                    "created_at": meta.get("created_at", ""),
                    "updated_at": meta.get("updated_at", ""),
                })
            except Exception as e:
                logger.warning(f"Failed to load meta for KB {kb_id}: {e}")
                result.append({
                    "id": kb_id,
                    "name": entry.get("name", kb_id),
                    "dataset_name": entry.get("dataset_name", ""),
                    "created_at": entry.get("created_at", ""),
                    "updated_at": entry.get("updated_at", ""),
                })
        else:
            result.append({
                "id": kb_id,
                "name": entry.get("name", kb_id),
                "dataset_name": entry.get("dataset_name", ""),
                "created_at": entry.get("created_at", ""),
                "updated_at": entry.get("updated_at", ""),
            })
    return result


def get_knowledge_base(kb_id: str) -> Optional[Dict[str, Any]]:
    """
    Get a single knowledge base by id.
    Returns { id, name, dataset_name, created_at, updated_at, schema, prompts } or None.
    """
    kb_path = _kb_dir(kb_id)
    if not kb_path.exists():
        return None
    meta_path = kb_path / META_FILE
    schema_path = kb_path / SCHEMA_FILE
    prompts_path = kb_path / PROMPTS_FILE
    result = {"id": kb_id}
    if meta_path.exists():
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            result.update(meta)
        except Exception as e:
            logger.warning(f"Failed to load meta for KB {kb_id}: {e}")
    if schema_path.exists():
        try:
            with open(schema_path, "r", encoding="utf-8") as f:
                result["schema"] = json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load schema for KB {kb_id}: {e}")
            result["schema"] = _default_schema()
    else:
        result["schema"] = _default_schema()
    if prompts_path.exists():
        try:
            with open(prompts_path, "r", encoding="utf-8") as f:
                result["prompts"] = json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load prompts for KB {kb_id}: {e}")
            result["prompts"] = _default_prompts()
    else:
        result["prompts"] = _default_prompts()
    return result


def _default_schema() -> Dict[str, List[str]]:
    """Default schema structure."""
    return {
        "Nodes": [
            "person", "location", "organization", "event", "object",
            "concept", "time_period", "creative_work", "biological_entity", "natural_phenomenon"
        ],
        "Relations": [
            "is_a", "part_of", "located_in", "created_by", "used_by", "participates_in",
            "related_to", "belongs_to", "influences", "precedes", "arrives_in", "comparable_to"
        ],
        "Attributes": [
            "name", "date", "size", "type", "description", "status",
            "quantity", "value", "position", "duration", "time"
        ]
    }


def _default_prompts() -> Dict[str, str]:
    """Default prompts (placeholders); real content should come from base_config or user."""
    return {
        "construction": "",
        "decomposition": "",
        "retrieval": ""
    }


def create_knowledge_base(
    name: str,
    dataset_name: str,
    schema: Optional[Dict[str, Any]] = None,
    prompts: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Create a new knowledge base.
    Returns the created KB dict.
    """
    _ensure_store()
    kb_id = str(uuid.uuid4())[:8]
    kb_path = _kb_dir(kb_id)
    kb_path.mkdir(parents=True, exist_ok=True)
    now = datetime.utcnow().isoformat() + "Z"
    meta = {
        "id": kb_id,
        "name": name,
        "dataset_name": dataset_name,
        "created_at": now,
        "updated_at": now,
    }
    with open(kb_path / META_FILE, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    schema_data = schema if schema is not None else _default_schema()
    with open(kb_path / SCHEMA_FILE, "w", encoding="utf-8") as f:
        json.dump(schema_data, f, ensure_ascii=False, indent=2)
    prompts_data = prompts if prompts is not None else _default_prompts()
    with open(kb_path / PROMPTS_FILE, "w", encoding="utf-8") as f:
        json.dump(prompts_data, f, ensure_ascii=False, indent=2)
    index = _load_index()
    index.append({"id": kb_id, "name": name, "dataset_name": dataset_name, "created_at": now, "updated_at": now})
    _save_index(index)
    return get_knowledge_base(kb_id)


def update_knowledge_base(
    kb_id: str,
    name: Optional[str] = None,
    dataset_name: Optional[str] = None,
    schema: Optional[Dict[str, Any]] = None,
    prompts: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Update an existing knowledge base. Only provided fields are updated.
    """
    kb_path = _kb_dir(kb_id)
    if not kb_path.exists():
        return None
    now = datetime.utcnow().isoformat() + "Z"
    meta_path = kb_path / META_FILE
    meta = {}
    if meta_path.exists():
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    if name is not None:
        meta["name"] = name
    if dataset_name is not None:
        meta["dataset_name"] = dataset_name
    meta["updated_at"] = now
    meta["id"] = kb_id
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    if schema is not None:
        with open(kb_path / SCHEMA_FILE, "w", encoding="utf-8") as f:
            json.dump(schema, f, ensure_ascii=False, indent=2)
    if prompts is not None:
        with open(kb_path / PROMPTS_FILE, "w", encoding="utf-8") as f:
            json.dump(prompts, f, ensure_ascii=False, indent=2)
    index = _load_index()
    for i, entry in enumerate(index):
        if entry.get("id") == kb_id:
            index[i] = {
                "id": kb_id,
                "name": meta.get("name", kb_id),
                "dataset_name": meta.get("dataset_name", ""),
                "created_at": meta.get("created_at", entry.get("created_at", "")),
                "updated_at": now,
            }
            break
    _save_index(index)
    return get_knowledge_base(kb_id)


def delete_knowledge_base(kb_id: str) -> bool:
    """Delete a knowledge base and its directory."""
    import shutil
    kb_path = _kb_dir(kb_id)
    if not kb_path.exists():
        return False
    shutil.rmtree(kb_path)
    index = _load_index()
    index = [e for e in index if e.get("id") != kb_id]
    _save_index(index)
    return True


def get_schema_path_for_kb(kb_id: str) -> Optional[str]:
    """
    Return the filesystem path to the KB's schema.json so that components
    that expect a schema_path can use it. Returns None if KB does not exist.
    """
    path = _kb_dir(kb_id) / SCHEMA_FILE
    return str(path) if path.exists() else None


def get_schema_dict_for_kb(kb_id: str) -> Optional[Dict[str, Any]]:
    """Load and return schema as dict for a KB. Returns None if KB does not exist."""
    kb = get_knowledge_base(kb_id)
    if kb is None:
        return None
    return kb.get("schema")


def get_prompts_for_kb(kb_id: str) -> Optional[Dict[str, str]]:
    """Load and return prompts (construction, decomposition, retrieval) for a KB."""
    kb = get_knowledge_base(kb_id)
    if kb is None:
        return None
    return kb.get("prompts")
