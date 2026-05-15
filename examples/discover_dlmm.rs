use stacks_dex_pools::dlmm::discover_dlmm_pools;
use stacks_dex_pools::{Principal, RpcConfig, StacksRpcClient};
use std::sync::Arc;

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let client = Arc::new(StacksRpcClient::new(RpcConfig {
        base_url: "https://node.bitflowapis.finance".to_string(),
        max_retries: 3,
        ..Default::default()
    })?);
    let core: Principal = "SP1PFR4V08H1RAZXREBGFFQ59WB739XM8VVGTFSEA.dlmm-core-v-1-1".parse()?;
    let pools = discover_dlmm_pools(client, &core, 8, None).await?;
    println!("discovered {} pool(s)", pools.len());
    for p in &pools {
        println!(
            "  id={:>3}  status={}  name={:<22} {}",
            p.id, p.status, p.name, p.pool_contract
        );
    }
    Ok(())
}
