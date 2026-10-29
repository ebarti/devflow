# Frozen source producer from published checkpoint v1 at 55ea327b3de564ab7731edb39012daabb60d3663.
# SHA256 of exact function: fe6e780afe773104fcf1cf64071bab7d7019d25dc750200e7a7c1f7963f4c35b
# Helpers are supplied by the authenticated runtime fixture.
# ruff: noqa: F821, E501

def _candidate_snapshot(spec, stage, inventory, resources):
    checkout = Path(spec["checkout"])
    from .delivery_filesystem_custody import matches

    if any(
        entry.get("generated_output_preservation")
        for raw, entry in resources["roots"].items()
        if Path(raw).is_relative_to(checkout)
    ):
        raise ValueError("checkpoint generated-source preservation needs custody reconciliation")
    generated = [
        Path(raw)
        for raw, entry in resources["roots"].items()
        if entry["kind"] == "generated"
        and Path(raw).is_relative_to(checkout)
        and Path(raw).exists()
        and matches(spec, Path(raw), entry["identity"])
    ]
    ignored = _git(checkout, "ls-files", "--others", "--ignored", "--exclude-standard", "-z")
    for raw in ignored.split(b"\0"):
        if not raw:
            continue
        relative = Path(os.fsdecode(raw))
        if (
            not _source_path_allowed(relative.as_posix())
            or any(part in SKIP_NAMES for part in relative.parts)
            or any((checkout / relative).is_relative_to(root) for root in generated)
        ):
            continue
        raise ValueError(
            "checkpoint has unclassified ignored source requiring preservation: "
            + relative.as_posix()
        )
    candidate = candidate_for(checkout)
    files = {}
    for path in _files(checkout):
        relative = path.relative_to(checkout).as_posix()
        if not _source_path_allowed(relative):
            raise ValueError("checkpoint refuses credential/profile source: " + relative)
        if path.is_symlink():
            files[relative] = {"kind": "symlink", "target": os.readlink(path)}
        else:
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
                raise ValueError(
                    "checkpoint source is not an owned unlinked regular file: " + relative
                )
            raw = path.read_bytes()
            artifact = "source/" + relative
            _artifact(stage, artifact, raw, inventory)
            files[relative] = {
                "kind": "file",
                "artifact": artifact,
                "executable": bool(info.st_mode & stat.S_IXUSR),
            }
    for name, args in (
        ("staged.patch", ("diff", "--cached", "--binary", "--full-index", "HEAD")),
        ("unstaged.patch", ("diff", "--binary", "--full-index")),
    ):
        _artifact(stage, name, _git(checkout, *args), inventory)
    bundle = stage / "candidate.bundle"
    _git(checkout, "bundle", "create", str(bundle), "HEAD")
    bundle.chmod(0o600)
    raw = bundle.read_bytes()
    inventory["candidate.bundle"] = {"sha256": _hash(raw), "size": len(raw)}
    if candidate_for(checkout) != candidate:
        raise ValueError("checkpoint candidate changed during preservation")
    return {
        "candidate": candidate,
        "files": files,
        "branch": _git(checkout, "symbolic-ref", "--short", "HEAD").decode().strip(),
        "index_tree": _git(checkout, "write-tree").decode().strip(),
    }

# Frozen v1 restore receipt producer; exact function SHA256: 284079cb25ffb719fc235adae60609941ff969decc24f1181e81965c7f5d195c
def _finish_restore(
    store, db, spec, value, checkpoint, request, key, intent, *, fence, writer=None,
    admission_context=None, checkpoint_context=None, registry_observation=None,
    graph_validation=None, attempt_evidence=None, host_observation=None, registry_projection=None,
):
    """Complete an acknowledged allocation intent, including a lost move response."""
    from .delivery_workspace_custody import source_for

    checkout = Path(spec["checkout"])
    staging = Path(intent["staging"])
    if (
        staging.parent != checkout.parent
        or not staging.name.startswith(".checkpoint-restore-")
        or staging.resolve() != staging
    ):
        raise ValueError("checkpoint restore intent escaped its allocation namespace")
    current = checkout if os.path.lexists(checkout) else staging
    if (
        not current.exists()
        or current.is_symlink()
        or _identity(current) != intent["prepared_identity"]
        or candidate_for(current) != value["source"]["candidate"]
        or _git(current, "write-tree").decode().strip() != value["source"]["index_tree"]
    ):
        raise ValueError("checkpoint restore allocation has an unknown or changed outcome")
    _openers(current)
    observed_ledger = _execution_ledger(
        spec, allow_unfinished_workers=fence is not None and writer is not None,
        facts=registry_observation.facts if registry_observation is not None else None,
    )
    with _checkpoint_admission(
        admission_context, registry_observation, host_observation
    ) as (registry_db, host_db, registry_fences):
        if not db.in_transaction:
            db.execute("BEGIN")
        row = _row(db, spec["run_id"])
        _restore_fence(db, row, value, fence)
        _verify_frozen_controllers(
            _frozen(db, row), value.get("controller"),
            fence=fence, checkpoint_context=checkpoint_context,
        )
        if not _same_snapshot(_frozen(db, row), value):
            raise ValueError("checkpoint run or budget changed during restoration")
        claim = store.state.claim_for(db, row["work_id"])
        if (dict(claim) if claim else None) != value["owner"] or not _same_ledger(
            observed_ledger, value.get("execution_ledger"),
        ):
            raise ValueError("checkpoint execution owner changed during restoration")
        if attempt_evidence is not None:
            attempt_evidence.verify(db, host_db)
        if graph_validation is not None:
            graph_validation(db, registry_db)
        _validate_registry(registry_fences)
        db.commit()
    source = source_for(spec)
    if (
        Path(
            _git(current, "rev-parse", "--path-format=absolute", "--git-common-dir")
            .decode()
            .strip()
        )
        != source
    ):
        raise ValueError("checkpoint restored workspace lost its independent Git owner")
    branch = value["source"]["branch"]
    if (
        _git(source, "rev-parse", "refs/heads/" + branch).decode().strip()
        != value["source"]["candidate"]["head"]
    ):
        raise ValueError("checkpoint local branch moved; reconcile before restoring")
    if current == staging:
        _restore_git(db, key, intent, source, "worktree", "prune")
        _restore_git(db, key, intent, staging, "symbolic-ref", "HEAD", "refs/heads/" + branch)
        _restore_git(db, key, intent, source, "worktree", "move", str(staging), str(checkout))
    for relative, item in value["state_refs"].items():
        target = _path(Path(spec["state_dir"]), relative)
        if item.get("kind") == "absent":
            if os.path.lexists(target):
                raise ValueError("checkpoint no-GO result is no longer absent: " + relative)
            continue
        raw = read_private(_root(spec) / value["checkpoint_id"] / item["artifact"], raw=True)
        _restore_file(db, key, intent, target, raw, item["mode"], mtime_ns=item["mtime_ns"])
        _sync_parents(target, Path(spec["state_dir"]))
    _sync_tree(checkout)
    observed_ledger = _execution_ledger(
        spec, allow_unfinished_workers=fence is not None and writer is not None,
        facts=registry_observation.facts if registry_observation is not None else None,
    )
    if not _same_ledger(observed_ledger, value.get("execution_ledger")):
        raise ValueError("checkpoint cumulative repair budget changed before acknowledgement")
    restored_identity = _identity(checkout)
    with _checkpoint_admission(
        admission_context, registry_observation, host_observation
    ) as (registry_db, host_db, registry_fences):
        db.execute("BEGIN IMMEDIATE")
        current_row = _row(db, spec["run_id"])
        if writer is not None:
            writer.verify(db)
        _restore_fence(db, current_row, value, fence)
        _verify_frozen_controllers(
            _frozen(db, current_row), value.get("controller"),
            fence=fence, checkpoint_context=checkpoint_context,
        )
        current_claim = store.state.claim_for(db, current_row["work_id"])
        if (
            not _same_snapshot(_frozen(db, current_row), value)
            or (dict(current_claim) if current_claim else None) != value["owner"]
        ):
            raise ValueError(
                "checkpoint admission state changed before restoration acknowledgement"
            )
        response = {
            "run_id": spec["run_id"],
            "checkpoint_id": value["checkpoint_id"],
            "manifest_sha256": checkpoint["manifest_sha256"],
            "original_identity": value["checkout_identity"],
            "restored_identity": restored_identity,
            "resource_manifest_sha256": value["resource_manifest_sha256"],
            "candidate": value["source"]["candidate"],
            "revision": request["expected_revision"],
            "restored": True,
            "execution_started": False,
        }
        db.execute(
            "INSERT INTO delivery_commands VALUES (?,?,?,?)",
            (key, spec["run_id"], digest(request), canonical_json(response)),
        )
        db.execute(
            "UPDATE delivery_mutations SET state='complete',response_json=? WHERE command_id=?",
            (canonical_json(response), key),
        )
        store._event(
            db,
            spec["run_id"],
            request["expected_revision"],
            RESTORED,
            "Delivery checkpoint restored into a new owned workspace",
            response,
            registry_db=registry_db,
            registry_projection=registry_projection,
        )
        if attempt_evidence is not None:
            attempt_evidence.verify(db, host_db)
        if graph_validation is not None:
            graph_validation(db, registry_db)
        _validate_registry(registry_fences)
        db.commit()
        return response
