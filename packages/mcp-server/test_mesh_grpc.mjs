import { TordialMeshClient } from "./src/tordialClient.mjs";

async function run() {
  console.log("[*] Initializing TordialMeshClient on 127.0.0.1:50055...");
  const client = new TordialMeshClient("127.0.0.1:50055");

  const telemetrySample = {
    latencyMs: 4.2,
    queueDepth: 2.5,
    thermalHeadroom: 0.015,
    batteryReserve: 0.88,
    packetLossRate: 0.002,
    bandwidthCapacity: 0.95,
    memoryPressure: 0.22,
    computeLoad: 0.08,
  };

  try {
    const t0 = performance.now();
    const result = await client.routeBurst(telemetrySample, 500, "SYNARA-MCP-AGENT");
    const t1 = performance.now();

    console.log("\n[+] Received gRPC RouteBurstResponse from Tordial-GS:");
    console.log(`    Target Node       : ${result.node_id}`);
    console.log(`    Budget Sats       : ${result.budget_sats}`);
    console.log(`    Dispatch Status   : ${result.decision.status}`);
    console.log(`    Root Index        : #${result.decision.selected_root_index}`);
    console.log(`    Dispatch Weight   : ${parseFloat(result.decision.dispatch_weight).toFixed(4)}`);
    console.log(`    Mass Norm         : ${parseFloat(result.decision.mass_norm).toFixed(4)}`);
    console.log(`    Settled Balance   : ${result.settled_balance_status}`);
    console.log(`    Total RPC Latency : ${(t1 - t0).toFixed(2)} ms`);
    console.log(`    Core Compute Time : ${(result.process_duration_ns / 1000).toFixed(2)} µs`);
  } catch (err) {
    console.error("[-] gRPC Dispatch Failed:", err.message);
    process.exitCode = 1;
  } finally {
    client.close();
  }
}

run();
