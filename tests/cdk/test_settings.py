from settings import StackSettings


def test_backfill_settings_defaults() -> None:
    settings = StackSettings(STAGE="dev", ACCOUNT_ID="111111111111")
    assert settings.BACKFILL_ENABLED is False
    assert settings.BACKFILL_PARTITION_SIZE == 500
    assert settings.BACKFILL_MAX_ITEMS_PER_BATCH == 10
    assert settings.BACKFILL_MAX_CONCURRENCY == 50


def test_forward_queue_enabled_defaults_on_when_backfill_off() -> None:
    settings = StackSettings(STAGE="dev", ACCOUNT_ID="111111111111")
    assert settings.FORWARD_QUEUE_ENABLED is True


def test_forward_queue_enabled_defaults_off_when_backfill_on() -> None:
    settings = StackSettings(
        STAGE="dev", ACCOUNT_ID="111111111111", BACKFILL_ENABLED=True
    )
    assert settings.FORWARD_QUEUE_ENABLED is False


def test_forward_queue_enabled_explicit_value_is_honored() -> None:
    settings = StackSettings(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        BACKFILL_ENABLED=True,
        FORWARD_QUEUE_ENABLED=True,
    )
    assert settings.FORWARD_QUEUE_ENABLED is True


def test_forward_queue_disabled_explicit_with_backfill_off() -> None:
    settings = StackSettings(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        FORWARD_QUEUE_ENABLED=False,
    )
    assert settings.FORWARD_QUEUE_ENABLED is False


def test_s3_prefix_scopes_the_icechunk_prefix() -> None:
    settings = StackSettings(
        STAGE="dev",
        ACCOUNT_ID="111111111111",
        S3_PREFIX="tempo",
        ICECHUNK_PREFIX="hcho-v04",
    )

    assert settings.s3_key_prefix == "tempo"
    assert settings.icechunk_storage_prefix == "tempo/hcho-v04"


def test_inventory_prefix_defaults_under_s3_prefix() -> None:
    s = StackSettings(STAGE="dev", S3_PREFIX="tempo/hcho", INVENTORY_PREFIX=None)
    assert s.inventory_prefix == "tempo/hcho/inventory"


def test_inventory_prefix_without_s3_prefix() -> None:
    s = StackSettings(STAGE="dev", S3_PREFIX=None, INVENTORY_PREFIX=None)
    assert s.inventory_prefix == "inventory"


def test_inventory_prefix_explicit_overrides_and_strips() -> None:
    s = StackSettings(STAGE="dev", S3_PREFIX="tempo/hcho", INVENTORY_PREFIX="/inv/")
    assert s.inventory_prefix == "inv"


def test_icechunk_prefix_must_be_relative_to_s3_prefix() -> None:
    import pytest

    with pytest.raises(ValueError, match="relative to S3_PREFIX"):
        StackSettings(
            STAGE="dev",
            ACCOUNT_ID="111111111111",
            S3_PREFIX="tempo",
            ICECHUNK_PREFIX="tempo/hcho-v04",
        )
