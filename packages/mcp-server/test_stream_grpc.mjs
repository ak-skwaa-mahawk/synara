import { TordialMeshClient } from "./src/tordialClient.mjs";

async function run() {
  console.log("[*] Connecting duplex stream to Tordial-GS on 127.0.0.1:50055...");
  const client = new TordialMeshClient("127.0.0.1:50055");
  const stream = client.createBurstStream();

  let receivedCount = 0;
  const targetCount = 10;
  const t0 = performance.now();

  stream.on("data", (response) => {
    receivedCount++;
    console.log(`[+] Stream Frame #${receivedCount}: Root=#${response.decision.selected_root_index} | Status=${response.decision.status} | Weight=${parseFloat(response.decision.dispatch_weight).toFixed(4)} | Latency=${(response.process_duration_ns / 1000).toFixed(2)} µs`);
    
    if (receivedCount === targetCount) {
      const elapsed = performance.now() - t0;
      console.log(`\n[✓] Successfully processed ${targetCount} duplex stream frames in ${elapsed.toFixed(2)} ms (${(elapsed / targetCount).toFixed(2)} ms/frame avg).`);
      stream.end();
      client.close();
    }
  });

  stream.on("error", (err) => {
    console.error("[-] Stream Error:", err.message);
    client.close();
    process.exit(1);
  });

  // Pump 10 sequential telemetry bursts through the active pipeline
  for (let i = 0; i < targetCount; i++) {
    stream.write({
      origin_node_id: `STREAM-WORKER-${i}`,
      budget_sats: 500,
      payload_digest: `digest_${Date.now()}_${i}`,
      timestamp_epoch_ms: Date.now(),
      telemetry: {
        latency_ms: 4.0 + (i * 0.02),
        queue_depth: 3.0,
        thermal_headroom: 0.01,
        battery_reserve: 0.02,
        packet_loss_rate: 3.5,
        bandwidth_capacity: 0.98,
        memory_pressure: 0.2,
        compute_load: 0.002,
      },
    });
  }
}

run();
