from __future__ import annotations

from dam_automation.snowflake import (
    SnowflakeProvisioner,
    SnowflakeConfig,
    SnowflakeDropSummary,
)


def _config() -> SnowflakeConfig:
    return SnowflakeConfig(
        account="acct",
        user="user",
        password="pass",
        role="role",
    )


def test_log_ddl_masks_sensitive_values(caplog) -> None:
    provisioner = SnowflakeProvisioner(_config())
    caplog.set_level("DEBUG")

    ddl = "CREATE SECRET VALUE 'super-secret'"
    provisioner._log_ddl(ddl, ["super-secret"])

    assert any("***" in message for message in caplog.messages)
    assert all("super-secret" not in message for message in caplog.messages)


def test_drop_summary_flags() -> None:
    summary = SnowflakeDropSummary(
        external_volume_dropped=True,
        catalog_integration_dropped=False,
        database_dropped=True,
    )

    assert summary.external_volume_dropped is True
    assert summary.catalog_integration_dropped is False
    assert summary.database_dropped is True
