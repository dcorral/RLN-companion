#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::time::Duration;

use common::*;
use serde_json::{json, Value};

struct Env {
    c1: Companion,
    _c2: Companion,
    sink1: WebhookSink,
    sink2: WebhookSink,
    r1: Rln,
    r2: Rln,
}

async fn setup() -> Env {
    init_tracing();
    let sink1 = WebhookSink::start().await;
    let sink2 = WebhookSink::start().await;
    let c1 = Companion::start(&rln_url(1), &sink1.url, |_| {}).await;
    let c2 = Companion::start(&rln_url(2), &sink2.url, |_| {}).await;
    let r1 = Rln::new(&c1.base_url);
    let r2 = Rln::new(&c2.base_url);
    for r in [&r1, &r2] {
        r.init().await;
        r.unlock().await;
        r.wait_synced(Duration::from_secs(60)).await;
    }
    Env {
        c1,
        _c2: c2,
        sink1,
        sink2,
        r1,
        r2,
    }
}

async fn issue_and_invoice(e: &Env) -> (String, String, i32) {
    fund(&e.r1).await;
    fund(&e.r2).await;
    let asset = e.r1.issue_nia("USDT", 1000).await;
    let (rid, _invoice, idx) = e.r2.rgb_invoice(None, None).await;
    (asset, rid, idx)
}

async fn await_settled(sink: &mut WebhookSink, who: &str) -> Value {
    sink.next_of("transfer.settled", Duration::from_secs(240))
        .await
        .unwrap_or_else(|| panic!("{who}: transfer.settled never arrived"))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn receive_flow_settles_with_webhooks() {
    let mut e = setup().await;
    let (asset, rid, _) = issue_and_invoice(&e).await;
    let txid = e.r1.send_rgb(&asset, &rid, 400, true).await;
    mine_and_index(&txid).await;

    let pending = e
        .sink2
        .next_of("transfer.confirmed_pending", Duration::from_secs(240))
        .await
        .expect("receiver transfer.confirmed_pending");
    assert_eq!(pending["transfer"]["recipient_id"], rid);
    assert_eq!(pending["transfer"]["status"], "WaitingConfirmations");

    let settled = await_settled(&mut e.sink2, "receiver").await;
    assert_eq!(settled["transfer"]["status"], "Settled");
    assert_eq!(settled["transfer"]["recipient_id"], rid);
    assert_eq!(settled["transfer"]["asset_id"], asset);
    assert!(settled["transfer"]["rln_idx"].is_number(), "{settled}");
    assert_eq!(settled["transfer"]["id"], pending["transfer"]["id"]);

    let id = settled["transfer"]["id"].as_str().unwrap();
    let rows = e.r2.companion_transfers().await;
    let row = rows.iter().find(|t| t["id"] == id).expect("row in mirror");
    assert_eq!(row["status"], "Settled");
    let rln_rows = e.r2.list_transfers(&asset).await;
    let rln_row = rln_rows
        .iter()
        .find(|t| t["recipient_id"] == rid)
        .expect("transfer in rln");
    assert_eq!(rln_row["status"], "Settled");
    assert_eq!(rln_row["idx"], settled["transfer"]["rln_idx"]);
    assert_eq!(e.r2.asset_balance(&asset).await, 400);

    let sent = await_settled(&mut e.sink1, "sender").await;
    assert_eq!(sent["transfer"]["kind"], "Send");
    assert_eq!(sent["transfer"]["txid"], txid);
    assert_eq!(sent["transfer"]["asset_id"], asset);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn non_donation_send_is_broadcast_by_sender_companion() {
    let mut e = setup().await;
    let (asset, rid, _) = issue_and_invoice(&e).await;
    let txid = e.r1.send_rgb(&asset, &rid, 400, false).await;
    assert!(
        !Bitcoind::mempool().await.contains(&txid),
        "tx broadcast too early"
    );

    wait_until(
        "sender companion broadcast",
        Duration::from_secs(60),
        || {
            let txid = txid.clone();
            async move { Bitcoind::mempool().await.contains(&txid) }
        },
    )
    .await;
    mine_and_index(&txid).await;

    let received = await_settled(&mut e.sink2, "receiver").await;
    assert_eq!(received["transfer"]["recipient_id"], rid);
    assert_eq!(received["transfer"]["asset_id"], asset);
    let sent = await_settled(&mut e.sink1, "sender").await;
    assert_eq!(sent["transfer"]["kind"], "Send");
    assert_eq!(sent["transfer"]["txid"], txid);
    assert_eq!(e.r2.asset_balance(&asset).await, 400);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn expired_invoice_is_reaped() {
    let mut e = setup().await;
    fund(&e.r2).await;
    let (rid, _, idx) = e.r2.rgb_invoice(None, Some(unix_now() + 3)).await;

    let failed = e
        .sink2
        .next_of("transfer.failed", Duration::from_secs(60))
        .await
        .expect("transfer.failed");
    assert_eq!(failed["transfer"]["recipient_id"], rid);
    assert_eq!(failed["transfer"]["batch_transfer_idx"], idx);
    assert_eq!(failed["previous_status"], "WaitingCounterparty");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn lightning_payment_webhooks() {
    let mut e = setup().await;
    fund(&e.r1).await;
    fund(&e.r2).await;
    let asset = e.r1.issue_nia("LNPAY", 1000).await;
    let node2_pubkey = e.r2.node_pubkey().await;

    e.r1.wait_height_synced().await;
    e.r1.open_channel(
        &format!("{node2_pubkey}@127.0.0.1:{}", peer2_port()),
        100_000,
        0,
        &asset,
        600,
    )
    .await;
    let funding_txid = poll_some("funding tx in mempool", Duration::from_secs(60), || async {
        let mempool = Bitcoind::mempool().await;
        e.r1.list_channels()
            .await
            .iter()
            .find(|c| c["asset_id"] == asset)
            .and_then(|c| c["funding_txid"].as_str().map(str::to_string))
            .filter(|txid| mempool.contains(txid))
    })
    .await;
    Bitcoind::mine(6).await;
    for (who, r) in [("opener", &e.r1), ("acceptor", &e.r2)] {
        wait_until(
            &format!("channel ready on {who}"),
            Duration::from_secs(120),
            || async {
                r.list_channels()
                    .await
                    .iter()
                    .any(|c| c["asset_id"] == asset && c["ready"] == true)
            },
        )
        .await;
    }

    let funding = e
        .sink1
        .next_of("transfer.settled", Duration::from_secs(120))
        .await
        .expect("funding transfer.settled on opener");
    assert_eq!(funding["transfer"]["kind"], "Send");
    assert_eq!(funding["transfer"]["asset_id"], asset);
    assert_eq!(funding["transfer"]["txid"], funding_txid.as_str());

    // Well above HTLC_MIN_MSAT so node2 can pay the invoice back over the channel
    // while keeping its 1%-of-capacity reserve.
    let ks_hash = e.r1.keysend(&node2_pubkey, 10_000_000, &asset, 10).await;
    let out = e
        .sink1
        .next_of("payment.settled", Duration::from_secs(120))
        .await
        .expect("outbound keysend payment.settled");
    assert_eq!(out["payment"]["payment_hash"], ks_hash);
    assert_eq!(out["payment"]["direction"], "Outbound");
    assert_eq!(out["payment"]["asset_amount"], 10);
    let inbound = e
        .sink2
        .next_of("payment.settled", Duration::from_secs(120))
        .await
        .expect("inbound keysend payment.settled");
    assert_eq!(inbound["payment"]["payment_hash"], ks_hash);
    assert_eq!(inbound["payment"]["direction"], "Inbound");
    assert_eq!(inbound["payment"]["asset_amount"], 10);

    let invoice = e.r1.ln_invoice(3_000_000, &asset, 5, 900).await;
    let pending = poll_some(
        "pre-tracked inbound payment row",
        Duration::from_secs(10),
        || async {
            e.r1.companion_payments().await.into_iter().find(|p| {
                p["status"] == "Pending" && p["direction"] == "Inbound" && p["asset_amount"] == 5
            })
        },
    )
    .await;
    let pay_hash = e.r2.send_payment(&invoice).await;
    assert_eq!(pending["payment_hash"], pay_hash);
    let inbound = e
        .sink1
        .next_of("payment.settled", Duration::from_secs(120))
        .await
        .expect("inbound invoice payment.settled");
    assert_eq!(inbound["payment"]["payment_hash"], pay_hash);
    assert_eq!(inbound["payment"]["direction"], "Inbound");
    assert_eq!(inbound["payment"]["asset_amount"], 5);
    let out = e
        .sink2
        .next_of("payment.settled", Duration::from_secs(120))
        .await
        .expect("outbound invoice payment.settled");
    assert_eq!(out["payment"]["payment_hash"], pay_hash);
    assert_eq!(out["payment"]["direction"], "Outbound");

    for r in [&e.r1, &e.r2] {
        wait_until(
            "pending_payments drained",
            Duration::from_secs(30),
            || async {
                let h = r.health().await;
                h["pending_payments"] == 0 && h["status"] == "ok"
            },
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn companion_started_against_locked_node_recovers() {
    let e = setup().await;
    let (asset, rid, _) = issue_and_invoice(&e).await;
    e.r2.lock().await;

    let mut sink = WebhookSink::start().await;
    let c = Companion::start(&rln_url(2), &sink.url, |_| {}).await;
    let r = Rln::new(&c.base_url);
    let health = r.health().await;
    assert_eq!(health["node"], "locked", "{health}");
    assert_eq!(health["status"], "degraded");
    assert!(health["last_full_sync_at"].is_null());

    r.unlock().await;
    r.wait_synced(Duration::from_secs(60)).await;
    assert_eq!(r.health().await["status"], "ok");

    let txid = e.r1.send_rgb(&asset, &rid, 400, true).await;
    mine_and_index(&txid).await;
    let settled = await_settled(&mut sink, "recovered companion").await;
    assert_eq!(settled["transfer"]["recipient_id"], rid);
    assert_eq!(settled["transfer"]["asset_id"], asset);
    assert_eq!(settled["transfer"]["status"], "Settled");
    assert_eq!(r.asset_balance(&asset).await, 400);
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn pass_through_parity() {
    let e = setup().await;
    let direct = rln_url(1);
    let client = reqwest::Client::new();
    let cases: [(&str, Option<Value>); 4] = [
        ("/nodeinfo", None),
        ("/listassets", Some(json!({"filter_asset_schemas": []}))),
        ("/btcbalance", Some(json!({"skip_sync": true}))),
        (
            "/listunspents",
            Some(json!({
                "settled_only": false,
                "skip_sync": true,
                "index_offset": null,
                "max_unspents": null
            })),
        ),
    ];
    for (path, body) in cases {
        let mut bodies = Vec::new();
        for base in [&e.c1.base_url, &direct] {
            let url = format!("{base}{path}");
            let req = match &body {
                Some(b) => client.post(&url).json(b),
                None => client.get(&url),
            };
            let resp = req.send().await.unwrap();
            assert_eq!(resp.status(), 200, "{url}");
            bodies.push(resp.bytes().await.unwrap());
        }
        assert_eq!(
            bodies[0],
            bodies[1],
            "{path}: companion {:?} vs rln {:?}",
            String::from_utf8_lossy(&bodies[0]),
            String::from_utf8_lossy(&bodies[1])
        );
    }
}
