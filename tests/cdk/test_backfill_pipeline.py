import aws_cdk as cdk
import aws_cdk.aws_s3 as s3
from aws_cdk.assertions import Match, Template
from conftest import actions_of, iam_statements, resources_of
from stack_constructs.backfill_pipeline import BackfillPipeline


def _template() -> Template:
    app = cdk.App()
    stack = cdk.Stack(
        app,
        "TestStack",
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )
    bucket = s3.Bucket(stack, "IceBucket")
    BackfillPipeline(
        stack,
        "Backfill",
        icechunk_bucket=bucket,
        icechunk_prefix=None,
        data_bucket_name="my-data-bucket",
        partition_size=500,
        max_items_per_batch=10,
        max_concurrency=50,
    )
    return Template.from_stack(stack)


def test_six_functions_with_cmd_overrides() -> None:
    template = _template()
    template.resource_count_is("AWS::Lambda::Function", 6)
    for action in ["partition", "init", "fork", "worker", "reduce", "promote"]:
        template.has_resource_properties(
            "AWS::Lambda::Function",
            Match.object_like(
                {"ImageConfig": {"Command": [f"backfill_handlers.{action}.handler"]}}
            ),
        )


def test_worker_has_data_bucket_read_and_list() -> None:
    template = _template()
    template.has_resource_properties(
        "AWS::IAM::Policy",
        Match.object_like(
            {
                "PolicyDocument": {
                    "Statement": Match.array_with(
                        [
                            Match.object_like(
                                {
                                    "Action": ["s3:GetObject", "s3:ListBucket"],
                                    "Resource": [
                                        "arn:aws:s3:::my-data-bucket/*",
                                        "arn:aws:s3:::my-data-bucket",
                                    ],
                                }
                            )
                        ]
                    )
                }
            }
        ),
    )


def _state_machine_asl() -> str:
    app = cdk.App()
    stack = cdk.Stack(
        app,
        "TestStack",
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )
    bucket = s3.Bucket(stack, "IceBucket")
    BackfillPipeline(
        stack,
        "Backfill",
        icechunk_bucket=bucket,
        icechunk_prefix=None,
        s3_prefix="tempo",
        data_bucket_name="my-data-bucket",
        partition_size=500,
        max_items_per_batch=10,
        max_concurrency=50,
    )
    tmpl = app.synth().get_stack_by_name("TestStack").template
    for res in tmpl["Resources"].values():
        if res["Type"] == "AWS::StepFunctions::StateMachine":
            parts = res["Properties"]["DefinitionString"]["Fn::Join"][1]
            return "".join(p if isinstance(p, str) else "<REF>" for p in parts)
    raise AssertionError("no state machine synthesized")


def test_state_machine_shape() -> None:
    template = _template()
    template.resource_count_is("AWS::StepFunctions::StateMachine", 1)

    asl = _state_machine_asl()
    # inner Distributed Map with a dynamic per-partition ItemReader key
    # (manifest_key comes from the partition item, not the fork result)
    assert '"Mode":"DISTRIBUTED"' in asl
    assert '"Key.$":"$.manifest_key"' in asl
    assert '"MaxItemsPerBatch":10' in asl
    # outer Map is serial
    assert '"MaxConcurrency":1' in asl
    # worker event reshape (Items -> file_keys, BatchInput carries the constants)
    assert '"file_keys.$":"$.Items"' in asl
    assert "$.BatchInput.fork_in_uri" in asl
    # ItemBatcher.BatchInput must use ".$" path keys so the fork URIs resolve at
    # runtime rather than being passed as the literal string "$.forkResult...".
    assert '"fork_in_uri.$":"$.forkResult.fork_in_uri"' in asl
    # reduce is reshaped to the flat event its handler expects
    assert '"forks_out_prefix.$":"$.forkResult.forks_out_prefix"' in asl
    # run_prefix is scoped under the configured global output prefix and
    # derives from the execution name
    assert "tempo/backfill" in asl
    assert "Execution.Name" in asl
    # InitTask forwards the whole execution state (no Parameters payload),
    # so the optional `force` flag reaches the handler and the documented
    # bare {"inventory_uri": ...} input stays valid. Only Partition and
    # Promote name inventory_uri explicitly.
    assert asl.count('"inventory_uri.$":"$.inventory_uri"') == 2


def test_init_and_promote_receive_the_inventory_uri() -> None:
    """Init builds the axis from the typed inventory (received via its
    whole-state passthrough, not an explicit Parameters key - see
    test_state_machine_shape) and promote gates the store against it via an
    explicit Parameters key, alongside partition's."""
    asl = _state_machine_asl()
    assert asl.count('"inventory_uri.$":"$.inventory_uri"') == 2  # partition, promote


def test_promote_receives_the_cas_expectation() -> None:
    """Promote's compare-and-swap needs the main tip Init branched from."""
    asl = _state_machine_asl()
    assert '"branched_from.$":"$.initResult.branched_from"' in asl


def test_worker_retries_transient_failures() -> None:
    """One object-store hiccup must not fail a multi-hour run outright."""
    asl = _state_machine_asl()
    assert '"ErrorEquals":["States.ALL"]' in asl
    assert '"MaxAttempts":2' in asl


STORE = "arn:<REF>:s3:::ice-test/tempo/hcho/v04/*"
RUN = "arn:<REF>:s3:::ice-test/tempo/hcho/backfill/*"
INVENTORY = "arn:<REF>:s3:::ice-test/tempo/hcho/inventory/*"


def _scoped_template() -> Template:
    app = cdk.App()
    stack = cdk.Stack(
        app,
        "TestStack",
        env=cdk.Environment(account="111111111111", region="us-east-1"),
    )
    bucket = s3.Bucket.from_bucket_name(stack, "IceBucket", "ice-test")
    BackfillPipeline(
        stack,
        "Backfill",
        icechunk_bucket=bucket,
        icechunk_prefix="tempo/hcho/v04",
        s3_prefix="tempo/hcho",
        inventory_prefix="tempo/hcho/inventory",
        data_bucket_name="my-data-bucket",
        partition_size=500,
        max_items_per_batch=10,
        max_concurrency=50,
    )
    return Template.from_stack(stack)


def test_partition_reads_inventory_and_writes_run_prefix_only() -> None:
    stmts = list(iam_statements(_scoped_template(), "partitionfn"))
    all_resources = [r for s in stmts for r in resources_of(s)]
    assert INVENTORY in all_resources
    assert STORE not in all_resources
    writes = [s for s in stmts if "s3:PutObject" in actions_of(s)]
    assert writes and all(resources_of(s) == [RUN] for s in writes)


def test_repo_lambdas_scoped_to_store_and_run_prefixes() -> None:
    template = _scoped_template()
    for action in ["init", "fork", "worker", "reduce", "promote"]:
        stmts = list(iam_statements(template, f"{action}fn"))
        writes = [s for s in stmts if "s3:PutObject" in actions_of(s)]
        assert writes, action
        assert all(resources_of(s) == [STORE, RUN] for s in writes), action
        assert any(
            "s3:ListBucket" in actions_of(s)
            and s.get("Condition")
            == {
                "StringLike": {
                    "s3:prefix": ["tempo/hcho/v04/*", "tempo/hcho/backfill/*"]
                }
            }
            for s in stmts
        ), action


def test_no_icechunk_prefix_keeps_bucket_wide_grant() -> None:
    stmts = list(iam_statements(_template(), "initfn"))
    assert any(
        "s3:DeleteObject*" in actions_of(s)
        and any(isinstance(r, str) and r.endswith("/*") for r in resources_of(s))
        for s in stmts
    )
