import path from "node:path";
import { fileURLToPath } from "node:url";
import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PROTO_PATH = path.resolve(__dirname, "../proto/router.proto");

const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const tordialProto = grpc.loadPackageDefinition(packageDefinition).tordial.v1;

export class TordialMeshClient {
  constructor(endpoint = "127.0.0.1:50055") {
    this.client = new tordialProto.SovereignMeshService(
      endpoint,
      grpc.credentials.createInsecure()
    );
  }

  /**
   * Route an agent execution burst through the 8D E8 state-space router.
   * @param {Object} telemetry - 8-dimensional operational telemetry.
   * @param {number} budgetSats - Sats allocated for the dispatch burst.
   * @param {string} originNode - Identifier of the dispatching agent node.
   * @returns {Promise<Object>} Routing decision and status metadata.
   */
  routeBurst(telemetry, budgetSats = 500, originNode = "SYNARA-MCP-01") {
    const request = {
      origin_node_id: originNode,
      budget_sats: budgetSats,
      payload_digest: `digest_${Date.now()}`,
      timestamp_epoch_ms: Date.now(),
      telemetry: {
        latency_ms: telemetry.latencyMs ?? 4.0,
        queue_depth: telemetry.queueDepth ?? 1.0,
        thermal_headroom: telemetry.thermalHeadroom ?? 0.05,
        battery_reserve: telemetry.batteryReserve ?? 0.95,
        packet_loss_rate: telemetry.packetLossRate ?? 0.01,
        bandwidth_capacity: telemetry.bandwidthCapacity ?? 0.9,
        memory_pressure: telemetry.memoryPressure ?? 0.2,
        compute_load: telemetry.computeLoad ?? 0.15,
      },
    };

    return new Promise((resolve, reject) => {
      this.client.RouteBurst(request, (err, response) => {
        if (err) {
          return reject(err);
        }
        resolve(response);
      });
    });
  }

  /**
   * Establish a bidirectional streaming pipeline for continuous high-rate bursts.
   * @returns {grpc.ClientDuplexStream} Duplex stream emitting RouteBurstResponse events.
   */
  createBurstStream() {
    return this.client.StreamRouteBursts();
  }

  close() {
    this.client.close();
  }
}
