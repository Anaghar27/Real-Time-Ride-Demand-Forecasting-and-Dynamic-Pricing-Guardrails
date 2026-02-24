# This module loads and validates pricing policy files used by the guardrails pipeline.
# It exists to keep policy behavior config-driven and prevent silent drift across environments.
# The loader performs schema checks and optional version pinning before runtime execution.
# It also persists policy snapshots to Postgres so every pricing run is auditable.

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import yaml
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.pricing_guardrails.pricing_config import PricingConfig


@dataclass(frozen=True)
class PolicyBundle:
    policy_version: str
    pricing_policy: dict[str, Any]
    multiplier_rules: dict[str, Any]
    rate_limit_rules: dict[str, Any]
    reason_codes: dict[str, Any]


REQUIRED_PRICING_POLICY_KEYS = {
    "pricing_policy_version",
    "default_floor_multiplier",
    "global_cap_multiplier",
    "max_increase_per_bucket",
    "max_decrease_per_bucket",
}
REQUIRED_MULTIPLIER_RULE_KEYS = {"policy_version", "active_method", "methods"}
REQUIRED_RATE_LIMIT_RULE_KEYS = {"policy_version", "max_increase_per_bucket", "max_decrease_per_bucket"}
REQUIRED_REASON_CODE_KEYS = {"policy_version", "codes", "priority_order"}


def _load_yaml(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"Policy file {path} must be a YAML mapping")
    return dict(loaded)


def _validate_required(config: dict[str, Any], required: set[str], path: str) -> None:
    missing = required.difference(config.keys())
    if missing:
        raise ValueError(f"Policy file {path} missing required keys: {sorted(missing)}")


def load_policy_bundle(
    *,
    pricing_config: PricingConfig,
    pricing_policy_path: str = "configs/pricing_policy.yaml",
    multiplier_rules_path: str = "configs/multiplier_rules.yaml",
    rate_limit_rules_path: str = "configs/rate_limit_rules.yaml",
    reason_codes_path: str = "configs/reason_codes.yaml",
) -> PolicyBundle:
    pricing_policy = _load_yaml(pricing_policy_path)
    multiplier_rules = _load_yaml(multiplier_rules_path)
    rate_limit_rules = _load_yaml(rate_limit_rules_path)
    reason_codes = _load_yaml(reason_codes_path)

    _validate_required(pricing_policy, REQUIRED_PRICING_POLICY_KEYS, pricing_policy_path)
    _validate_required(multiplier_rules, REQUIRED_MULTIPLIER_RULE_KEYS, multiplier_rules_path)
    _validate_required(rate_limit_rules, REQUIRED_RATE_LIMIT_RULE_KEYS, rate_limit_rules_path)
    _validate_required(reason_codes, REQUIRED_REASON_CODE_KEYS, reason_codes_path)

    bundle = PolicyBundle(
        policy_version=pricing_config.pricing_policy_version,
        pricing_policy=pricing_policy,
        multiplier_rules=multiplier_rules,
        rate_limit_rules=rate_limit_rules,
        reason_codes=reason_codes,
    )
    validate_policy_bundle(bundle=bundle, pricing_config=pricing_config)
    return bundle


def validate_policy_bundle(*, bundle: PolicyBundle, pricing_config: PricingConfig) -> None:
    configured_version = pricing_config.pricing_policy_version
    policy_versions = {
        str(bundle.pricing_policy.get("pricing_policy_version", "")),
        str(bundle.multiplier_rules.get("policy_version", "")),
        str(bundle.rate_limit_rules.get("policy_version", "")),
        str(bundle.reason_codes.get("policy_version", "")),
    }
    if len(policy_versions) != 1:
        raise ValueError(f"Policy files disagree on version: {sorted(policy_versions)}")

    policy_version = next(iter(policy_versions))
    if policy_version != configured_version:
        raise ValueError(
            "Configured PRICING_POLICY_VERSION does not match policy YAML version: "
            f"configured={configured_version!r} file_version={policy_version!r}"
        )

    active_method = str(bundle.multiplier_rules.get("active_method", ""))
    methods = bundle.multiplier_rules.get("methods", {})
    if active_method not in methods:
        raise ValueError(f"multiplier_rules active_method={active_method!r} missing from methods block")

    codes = bundle.reason_codes.get("codes", {})
    priority_order = list(bundle.reason_codes.get("priority_order", []))
    if not codes:
        raise ValueError("reason_codes.yaml must define at least one reason code")
    for code in priority_order:
        if code not in codes:
            raise ValueError(f"reason_codes priority_order includes unknown code {code!r}")


def persist_policy_snapshots(
    *,
    engine: Engine,
    bundle: PolicyBundle,
    effective_from: datetime | None = None,
) -> None:
    effective_ts = effective_from or datetime.now(tz=UTC)

    statement = text(
        """
        INSERT INTO pricing_policy_snapshot (policy_version, config_json, effective_from, active_flag)
        VALUES (:policy_version, CAST(:config_json AS JSONB), :effective_from, TRUE)
        ON CONFLICT (policy_version, effective_from) DO UPDATE SET
            config_json = EXCLUDED.config_json,
            active_flag = EXCLUDED.active_flag,
            created_at = NOW()
        """
    )
    multiplier_statement = text(
        """
        INSERT INTO multiplier_rule_snapshot (policy_version, config_json, effective_from, active_flag)
        VALUES (:policy_version, CAST(:config_json AS JSONB), :effective_from, TRUE)
        ON CONFLICT (policy_version, effective_from) DO UPDATE SET
            config_json = EXCLUDED.config_json,
            active_flag = EXCLUDED.active_flag,
            created_at = NOW()
        """
    )
    rate_statement = text(
        """
        INSERT INTO rate_limit_rule_snapshot (policy_version, config_json, effective_from, active_flag)
        VALUES (:policy_version, CAST(:config_json AS JSONB), :effective_from, TRUE)
        ON CONFLICT (policy_version, effective_from) DO UPDATE SET
            config_json = EXCLUDED.config_json,
            active_flag = EXCLUDED.active_flag,
            created_at = NOW()
        """
    )

    with engine.begin() as connection:
        connection.execute(
            statement,
            {
                "policy_version": bundle.policy_version,
                "config_json": json.dumps(bundle.pricing_policy),
                "effective_from": effective_ts,
            },
        )
        connection.execute(
            multiplier_statement,
            {
                "policy_version": bundle.policy_version,
                "config_json": json.dumps(bundle.multiplier_rules),
                "effective_from": effective_ts,
            },
        )
        connection.execute(
            rate_statement,
            {
                "policy_version": bundle.policy_version,
                "config_json": json.dumps(bundle.rate_limit_rules),
                "effective_from": effective_ts,
            },
        )


def upsert_reason_code_reference(*, engine: Engine, bundle: PolicyBundle) -> int:
    codes = dict(bundle.reason_codes.get("codes", {}))
    if not codes:
        return 0

    payload: list[dict[str, Any]] = []
    for reason_code, details in codes.items():
        item = dict(details) if isinstance(details, dict) else {}
        payload.append(
            {
                "reason_code": str(reason_code),
                "category": str(item.get("category", "uncategorized")),
                "description": str(item.get("description", "")),
            }
        )

    statement = text(
        """
        INSERT INTO reason_code_reference (reason_code, category, description, active_flag)
        VALUES (:reason_code, :category, :description, TRUE)
        ON CONFLICT (reason_code) DO UPDATE SET
            category = EXCLUDED.category,
            description = EXCLUDED.description,
            active_flag = TRUE
        """
    )
    with engine.begin() as connection:
        connection.execute(statement, payload)
    return len(payload)
