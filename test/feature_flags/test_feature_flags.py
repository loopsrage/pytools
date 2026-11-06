from feature_flags.feature_flags import FeatureFlag, FeatureFlags, _bucket


def test_bucket_is_consistent_and_in_range():
    b1 = _bucket("user-1", "checkout_beta")
    b2 = _bucket("user-1", "checkout_beta")
    assert b1 == b2
    assert 0.0 <= b1 <= 100.0


def test_create_and_get_flag():
    ff = FeatureFlags("test_flags")
    flag = FeatureFlag("checkout_beta", enabled=True, rollout_percentage=50.0)
    ff.create(flag)
    fetched = ff.get("checkout_beta")
    assert fetched is not None
    assert fetched.name == "checkout_beta"
    assert fetched.rollout_percentage == 50.0


def test_create_duplicate_raises():
    ff = FeatureFlags("test_dup")
    ff.create(FeatureFlag("x", rollout_percentage=50.0))
    try:
        ff.create(FeatureFlag("x", rollout_percentage=50.0))
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError")


def test_is_enabled_explicit_user():
    ff = FeatureFlags("test_user")
    ff.create(FeatureFlag("beta", enabled=True, target_users=["alice"]))
    assert ff.is_enabled("beta", user_id="alice") is True
    assert ff.is_enabled("beta", user_id="bob") is False


def test_is_enabled_attribute_targeting():
    ff = FeatureFlags("test_attr")
    ff.create(
        FeatureFlag(
            "enterprise",
            enabled=True,
            target_attributes={"tier": "enterprise"},
        )
    )
    assert ff.is_enabled("enterprise", attributes={"tier": "enterprise"}) is True
    assert ff.is_enabled("enterprise", attributes={"tier": "free"}) is False


def test_is_enabled_percentage_rollout():
    ff = FeatureFlags("test_pct")
    # 100% rollout -> everyone gets it
    ff.create(FeatureFlag("all", enabled=True, rollout_percentage=100.0))
    assert ff.is_enabled("all", user_id="anyone") is True
    # 0% rollout -> no one gets it
    ff.create(FeatureFlag("none", enabled=True, rollout_percentage=0.0))
    assert ff.is_enabled("none", user_id="anyone") is False


def test_disabled_flag():
    ff = FeatureFlags("test_disabled")
    ff.create(FeatureFlag("off", enabled=False, rollout_percentage=100.0))
    assert ff.is_enabled("off", user_id="anyone") is False


def test_list_flags():
    ff = FeatureFlags("test_list")
    ff.create(FeatureFlag("a", rollout_percentage=10.0))
    ff.create(FeatureFlag("b", rollout_percentage=20.0))
    names = {f.name for f in ff.list()}
    assert names == {"a", "b"}


def test_delete_flag():
    ff = FeatureFlags("test_delete")
    ff.create(FeatureFlag("temp", rollout_percentage=10.0))
    assert ff.delete("temp") is True
    assert ff.get("temp") is None
    assert ff.delete("temp") is False
