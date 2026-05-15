//! Quote a Bitflow DLMM pool live.
//!
//! Run:
//!   cargo run --example quote_dlmm --features rpc -- 100      # 100 STX → USDCx, bps-10
//!   cargo run --example quote_dlmm --features rpc -- 1 10 100
//!
//! Compares to `python3 test/estimate_swap_dlmm.py 100 --tier 10` — should
//! match to the last digit (modulo pool state drift between the two reads).

use std::sync::Arc;

use anyhow::Result;
use stacks_dex_pools::dlmm::fetcher::{fetch_dlmm_pool, BootstrapMode};
use stacks_dex_pools::pool::principal::Principal;
use stacks_dex_pools::rpc::client::{RpcConfig, StacksRpcClient};
use stacks_dex_pools::token_info::StacksTokenInfo;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let amounts_human: Vec<f64> = if args.is_empty() {
        vec![100.0]
    } else {
        args.iter()
            .map(|s| s.parse().expect("amount must be a number"))
            .collect()
    };

    let pool_contract: Principal =
        "SM1FKXGNZJWSTWDWXQZJNF7B5TV5ZB235JTCXYXKD.dlmm-pool-stx-usdcx-v-1-bps-10".parse()?;
    let core_contract: Principal =
        "SP1PFR4V08H1RAZXREBGFFQ59WB739XM8VVGTFSEA.dlmm-core-v-1-1".parse()?;

    let client = Arc::new(StacksRpcClient::new(RpcConfig::default())?);
    let token_info = StacksTokenInfo::new(client.clone());

    println!("Bootstrapping {} (±10 bin window)…", pool_contract);
    let pool = fetch_dlmm_pool(
        client,
        &pool_contract,
        &core_contract,
        &token_info,
        BootstrapMode::default(),
        8,
        None,
    )
    .await?;
    println!(
        "  active bin {}, x_decimals={}, y_decimals={}, bins mirrored={}",
        pool.active_bin_id,
        pool.x_decimals,
        pool.y_decimals,
        pool.bins.len()
    );
    println!(
        "  fees (x→y): protocol={} provider={} variable={} (total {} bps)",
        pool.x_protocol_fee,
        pool.x_provider_fee,
        pool.x_variable_fee,
        pool.x_fee_bps()
    );

    let scale_x = 10u128.pow(pool.x_decimals as u32);
    let scale_y = 10u128.pow(pool.y_decimals as u32);

    println!();
    println!(
        "  {:>14}  {:>14}  {:>16}",
        "STX in", "USDCx out", "eff price (USD/STX)"
    );
    println!(
        "  {:>14}  {:>14}  {:>16}",
        "------", "---------", "-------------------"
    );
    for human in amounts_human {
        let raw = (human * scale_x as f64) as u128;
        let (dy, _last_bin, exhausted) = pool.quote_x_for_y(raw);
        let dy_human = dy as f64 / scale_y as f64;
        let price = if raw > 0 { dy_human / human } else { 0.0 };
        let edge = if exhausted {
            " ⚠ window exhausted"
        } else {
            ""
        };
        println!(
            "  {:>14.6}  {:>14.6}  {:>16.8}{}",
            human, dy_human, price, edge
        );
    }
    Ok(())
}
