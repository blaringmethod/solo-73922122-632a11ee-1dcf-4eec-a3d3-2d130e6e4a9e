"""Duplicate candidate pairs and reviewer verdicts.

The business rule under test:

* candidates are pairs of *different* identifiers whose title and creator,
  after trimming and case-folding, are equal;
* a verdict is valid only for the published content it was made against.  A
  correction (title/creator change) or a withdrawal expires it; publishing the
  same text again later must not revive it;
* re-reading the same content inside an inclusive window keeps it valid;
* a stale form submission is rejected and shows the current candidates;
* failed or cancelled harvests do not affect verdicts.
"""

from __future__ import annotations

import asyncio

import httpx
from fastapi.testclient import TestClient

from app.main import create_app
from app.oai import OaiClient
from conftest import wait_for_outcome


def publish(store, identifier, title, creator, datestamp, deleted=False):
    """Publish one record directly through a committed run (no source needed)."""
    run = store.create_run()
    store.set_window(run["id"], datestamp)
    store.stage_records(
        run["id"],
        [
            {
                "identifier": identifier,
                "title": title,
                "creator": creator,
                "datestamp": datestamp,
                "deleted": deleted,
            }
        ],
        0,
    )
    assert store.commit_run(run["id"])["committed"]


def pair_tokens(overview):
    assert overview["pending"], overview
    pair = overview["pending"][0]
    return (
        pair["left"]["identifier"],
        pair["right"]["identifier"],
        pair["left"]["content_token"],
        pair["right"]["content_token"],
    )


def judge(store, overview, verdict="same"):
    left, right, left_token, right_token = pair_tokens(overview)
    return store.record_judgement(left, right, left_token, right_token, verdict)


def judge_pair(store, overview, identifier_a, identifier_b, verdict="same"):
    """Judge one specific candidate pair out of a larger overview."""
    for pair in overview["pending"]:
        tokens = {
            pair["left"]["identifier"]: pair["left"]["content_token"],
            pair["right"]["identifier"]: pair["right"]["content_token"],
        }
        if set(tokens) == {identifier_a, identifier_b}:
            return store.record_judgement(
                identifier_a,
                identifier_b,
                tokens[identifier_a],
                tokens[identifier_b],
                verdict,
            )
    raise AssertionError(f"pair {identifier_a}/{identifier_b} not pending")


def test_candidates_use_trimmed_casefolded_title_and_creator(store):
    # Only leading/trailing whitespace is stripped; interior spacing is kept.
    publish(store, "oai:d:1", "  Duplicate Title ", "Alice ", "2026-01-01T00:00:01Z")
    publish(store, "oai:d:2", "duplicate title", " ALICE", "2026-01-01T00:00:02Z")
    # Same title, different creator: not a pair.
    publish(store, "oai:d:3", "duplicate title", "Bob", "2026-01-01T00:00:03Z")
    # Same identifier can never pair with itself; a single row is not a pair.
    publish(store, "oai:d:4", "unique", None, "2026-01-01T00:00:04Z")

    overview = store.duplicate_overview()
    assert overview["counts"]["pending"] == 1
    pair = overview["pending"][0]
    assert {pair["left"]["identifier"], pair["right"]["identifier"]} == {
        "oai:d:1",
        "oai:d:2",
    }
    assert overview["valid"] == []


def test_three_way_group_yields_all_pairs_and_each_is_removed_after_judging(store):
    for i, identifier in enumerate(["oai:g:1", "oai:g:2", "oai:g:3"], start=1):
        publish(store, identifier, "同一标题", "同一作者", f"2026-01-01T00:00:0{i}Z")
    overview = store.duplicate_overview()
    assert overview["counts"]["pending"] == 3  # C(3,2)

    result = judge(store, overview)
    assert result["accepted"] is True
    overview = result["overview"]
    assert overview["counts"]["pending"] == 2
    assert overview["counts"]["valid"] == 1
    assert overview["valid"][0]["verdict"] == "same"


def test_same_identifier_cannot_pair_with_itself(store):
    publish(store, "oai:solo", "标题", "作者", "2026-01-01T00:00:01Z")
    assert store.duplicate_overview()["pending"] == []
    result = store.record_judgement(
        "oai:solo", "oai:solo", "x", "x", "same"
    )
    assert result["accepted"] is False


def test_verdict_is_case_and_whitespace_insensitive_for_grouping(store):
    publish(store, "oai:w:1", "Title One", "Author", "2026-01-01T00:00:01Z")
    publish(store, "oai:w:2", "title one", "author", "2026-01-01T00:00:02Z")
    overview = store.duplicate_overview()
    assert judge(store, overview, "different")["accepted"] is True
    assert store.duplicate_overview()["counts"]["valid"] == 1


def test_correction_expires_verdict_and_same_text_restore_cannot_revive(store):
    publish(store, "oai:c:1", "重复标题", "作者甲", "2026-01-01T00:00:01Z")
    publish(store, "oai:c:2", "重复标题", "作者甲", "2026-01-01T00:00:02Z")
    assert judge(store, store.duplicate_overview())["accepted"] is True
    assert store.duplicate_overview()["counts"]["valid"] == 1

    # Correct the title, then harvest publishes the correction.
    publish(store, "oai:c:2", "更正后的标题", "作者甲", "2026-01-01T00:00:03Z")
    overview = store.duplicate_overview()
    assert overview["counts"]["valid"] == 0
    # No longer the same normalised combination -> no candidate either.
    assert overview["counts"]["pending"] == 0

    # Restore the original text (newer datestamp).  It becomes a candidate
    # again, but the old judgement must NOT come back.
    publish(store, "oai:c:2", "重复标题", "作者甲", "2026-01-01T00:00:04Z")
    overview = store.duplicate_overview()
    assert overview["counts"]["valid"] == 0
    assert overview["counts"]["pending"] == 1


def test_withdraw_expires_verdict_and_restore_does_not_revive(store):
    publish(store, "oai:x:1", "撤回标题", "作者", "2026-01-01T00:00:01Z")
    publish(store, "oai:x:2", "撤回标题", "作者", "2026-01-01T00:00:02Z")
    assert judge(store, store.duplicate_overview(), "different")["accepted"] is True

    publish(store, "oai:x:2", "", None, "2026-01-01T00:00:03Z", deleted=True)
    overview = store.duplicate_overview()
    assert overview["counts"]["valid"] == 0
    assert overview["counts"]["pending"] == 0  # only one record left

    publish(store, "oai:x:2", "撤回标题", "作者", "2026-01-01T00:00:04Z")
    overview = store.duplicate_overview()
    assert overview["counts"]["valid"] == 0
    assert overview["counts"]["pending"] == 1


def test_inclusive_window_reread_of_identical_content_keeps_verdict(store):
    publish(store, "oai:r:1", "闭区间标题", "作者", "2026-01-01T00:00:01Z")
    publish(store, "oai:r:2", "闭区间标题", "作者", "2026-01-01T00:00:02Z")
    assert judge(store, store.duplicate_overview())["accepted"] is True

    # Same title/creator re-published (boundary second re-read / touch).
    publish(store, "oai:r:1", "闭区间标题", "作者", "2026-01-01T00:00:02Z")
    overview = store.duplicate_overview()
    assert overview["counts"]["valid"] == 1
    assert overview["counts"]["pending"] == 0


def test_stale_submission_is_rejected_and_returns_current_candidates(store):
    publish(store, "oai:st:1", "过期标题", "作者", "2026-01-01T00:00:01Z")
    publish(store, "oai:st:2", "过期标题", "作者", "2026-01-01T00:00:02Z")
    overview = store.duplicate_overview()
    left, right, left_token, right_token = pair_tokens(overview)

    # Content changes after the page was rendered.
    publish(store, "oai:st:2", "改过的标题", "作者", "2026-01-01T00:00:03Z")

    result = store.record_judgement(left, right, left_token, right_token, "same")
    assert result["accepted"] is False
    assert "过期" in result["reason"] or "不再" in result["reason"]
    fresh = result["overview"]
    assert fresh["counts"]["valid"] == 0
    assert fresh["counts"]["pending"] == 0


def test_web_form_and_api_flow(web_app_like):
    app, store = web_app_like
    publish(store, "oai:web:1", "网页标题", "作者", "2026-01-01T00:00:01Z")
    publish(store, "oai:web:2", "网页标题", "作者", "2026-01-01T00:00:02Z")

    with TestClient(app) as client:
        page = client.get("/duplicates")
        assert page.status_code == 200
        assert "网页标题" in page.text
        api = client.get("/api/duplicates").json()
        pair = api["pending"][0]

        # HTML form verdict -> redirect + visible in valid list.
        response = client.post(
            "/duplicates/judgements",
            data={
                "left_identifier": pair["left"]["identifier"],
                "right_identifier": pair["right"]["identifier"],
                "left_token": pair["left"]["content_token"],
                "right_token": pair["right"]["content_token"],
                "verdict": "same",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        page = client.get("/duplicates")
        assert "同一成果" in page.text
        assert client.get("/api/duplicates").json()["counts"]["valid"] == 1

        # Stale JSON submission -> 409 with current candidates.
        response = client.post(
            "/api/duplicates/judgements",
            json={
                "left_identifier": pair["left"]["identifier"],
                "right_identifier": pair["right"]["identifier"],
                "left_token": "stale-token",
                "right_token": pair["right"]["content_token"],
                "verdict": "different",
            },
        )
        assert response.status_code == 409
        body = response.json()
        assert body["accepted"] is False
        assert "duplicates" in body


def test_web_stale_form_redirects_with_error_and_no_verdict(web_app_like):
    app, store = web_app_like
    publish(store, "oai:wf:1", "表单标题", "作者", "2026-01-01T00:00:01Z")
    publish(store, "oai:wf:2", "表单标题", "作者", "2026-01-01T00:00:02Z")
    with TestClient(app) as client:
        pair = client.get("/api/duplicates").json()["pending"][0]
        publish(store, "oai:wf:1", "新标题", "作者", "2026-01-01T00:00:03Z")
        response = client.post(
            "/duplicates/judgements",
            data={
                "left_identifier": pair["left"]["identifier"],
                "right_identifier": pair["right"]["identifier"],
                "left_token": pair["left"]["content_token"],
                "right_token": pair["right"]["content_token"],
                "verdict": "same",
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert "error=" in response.headers["location"]
        assert client.get("/api/duplicates").json()["counts"]["valid"] == 0


def test_duplicates_page_renders_when_empty(web_app_like):
    app, _ = web_app_like
    with TestClient(app) as client:
        page = client.get("/duplicates")
        assert page.status_code == 200
        assert "没有待核查的候选对" in page.text
        assert client.get("/api/duplicates").json()["counts"] == {
            "pending": 0,
            "valid": 0,
        }


def test_api_rejects_unknown_verdict(web_app_like):
    app, store = web_app_like
    publish(store, "oai:v:1", "校验标题", "作者", "2026-01-01T00:00:01Z")
    publish(store, "oai:v:2", "校验标题", "作者", "2026-01-01T00:00:02Z")
    with TestClient(app) as client:
        pair = client.get("/api/duplicates").json()["pending"][0]
        response = client.post(
            "/api/duplicates/judgements",
            json={
                "left_identifier": pair["left"]["identifier"],
                "right_identifier": pair["right"]["identifier"],
                "left_token": pair["left"]["content_token"],
                "right_token": pair["right"]["content_token"],
                "verdict": "maybe",
            },
        )
        assert response.status_code == 400
        assert client.get("/api/duplicates").json()["counts"]["valid"] == 0


async def test_failed_harvest_keeps_verdict(store, harvester, repo, monkeypatch):
    publish(store, "oai:f:1", "失败标题", "作者", "2026-01-01T00:00:01Z")
    publish(store, "oai:f:2", "失败标题", "作者", "2026-01-01T00:00:02Z")
    assert judge(store, store.duplicate_overview())["accepted"] is True

    await asyncio.sleep(1.05)
    repo.add_record(title="失败标题", creator="作者")  # would join the group

    class Boom(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            raise httpx.ConnectError("boom", request=request)

    harvester._client_factory = lambda: OaiClient(
        "http://127.0.0.1:9/oai", transport=Boom()
    )
    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "failed"

    overview = store.duplicate_overview()
    assert overview["counts"]["valid"] == 1  # verdict untouched
    # The failed round published nothing, so no new candidate appears.
    assert overview["counts"]["pending"] == 0


async def test_cancelled_harvest_keeps_verdict(store, harvester, repo):
    publish(store, "oai:k:1", "取消标题", "作者", "2026-01-01T00:00:01Z")
    publish(store, "oai:k:2", "取消标题", "作者", "2026-01-01T00:00:02Z")
    assert judge(store, store.duplicate_overview(), "different")["accepted"] is True

    await asyncio.sleep(1.05)
    repo.add_record(title="取消标题", creator="作者")
    repo.set_meta("slow_seconds", "3")
    control = await harvester.start_run()
    await asyncio.sleep(0.2)
    await harvester.cancel_run(control.run_id, "测试取消", "user")
    await wait_for_outcome(control)

    overview = store.duplicate_overview()
    assert overview["counts"]["valid"] == 1
    assert overview["valid"][0]["verdict"] == "different"
    # The source record was never published, so it is not a candidate.
    assert overview["counts"]["pending"] == 0


async def test_harvest_correction_expires_verdict_end_to_end(
    store, harvester, repo
):
    """A correction arriving through a real harvest expires the verdict."""
    duplicate = repo.add_record(
        title="数字人文中的文本挖掘方法", creator="王小明; 李静"
    )["identifier"]

    first = await harvester.start_run()
    assert await wait_for_outcome(first) == "succeeded"
    overview = store.duplicate_overview()
    # The demo seed also contains a deliberate duplicate; only the pair this
    # test created matters here.
    assert run_pair_present(overview, "oai:demo:0001", duplicate)
    assert judge_pair(
        store, overview, "oai:demo:0001", duplicate
    )["accepted"] is True

    await asyncio.sleep(1.05)
    second = await harvester.start_run()
    assert await wait_for_outcome(second) == "succeeded"
    # Re-reading identical content must not expire anything.
    assert run_pair_valid(store, "oai:demo:0001", duplicate)

    await asyncio.sleep(1.05)
    repo.touch_record(duplicate, title="数字人文中的文本挖掘方法（修订版）")
    third = await harvester.start_run()
    assert await wait_for_outcome(third) == "succeeded"

    overview = store.duplicate_overview()
    assert not run_pair_valid(store, "oai:demo:0001", duplicate)


def run_pair_present(overview, identifier_a, identifier_b):
    return any(
        {pair["left"]["identifier"], pair["right"]["identifier"]}
        == {identifier_a, identifier_b}
        for pair in overview["pending"]
    )


def run_pair_valid(store, identifier_a, identifier_b):
    return any(
        {item["left"]["identifier"], item["right"]["identifier"]}
        == {identifier_a, identifier_b}
        for item in store.duplicate_overview()["valid"]
    )
