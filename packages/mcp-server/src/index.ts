import { spawn } from "child_process";
import { TordialMeshClient } from "./tordialClient.mjs";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { readFile } from "fs/promises";
import { resolve } from "path";

const PROTO_PATH = resolve(process.env.HOME || "/data/data/com.termux/files/home", "Synara-core/proto/inference.proto");
const ANCHOR_PATH = resolve(process.env.HOME || "/data/data/com.termux/files/home", "Synara-core/active_scrp_anchor.json");
const GRPC_ENDPOINT = process.env.ISST_GRPC_ENDPOINT || "127.0.0.1:50051";

// Load Protobuf definitions
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});

const protoDescriptor = grpc.loadPackageDefinition(packageDefinition) as any;
const issttoft = protoDescriptor.issttoft;
const TORDIAL_GRPC_ENDPOINT = process.env.TORDIAL_GRPC_ENDPOINT || "127.0.0.1:50055";
const tordialClient = new TordialMeshClient(TORDIAL_GRPC_ENDPOINT);

const client = new issttoft.InferenceService(
  GRPC_ENDPOINT,
  grpc.credentials.createInsecure()
);

const server = new Server(
  {
    name: "synara-mcp-engine",
    version: "1.0.0",
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

// 1. Declare available tools
server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "update_peer_telemetry",
        description: "Ingest live 8D operational telemetry for an edge peer node into the Tordial-GS state tracker.",
        inputSchema: {
          type: "object",
          properties: {
            nodeId: { type: "string", description: "Node identifier (e.g. PEER-EDGE-01)" },
            latencyMs: { type: "number", description: "Network round-trip latency in ms (default: 4.0)" },
            queueDepth: { type: "number", description: "Worker queue depth (default: 1.0)" },
            thermalHeadroom: { type: "number", description: "Thermal headroom fraction 0.0-1.0 (default: 0.05)" },
            batteryReserve: { type: "number", description: "Battery capacity fraction 0.0-1.0 (default: 0.95)" },
            packetLossRate: { type: "number", description: "Link packet loss rate (default: 0.01)" },
            bandwidthCapacity: { type: "number", description: "Bandwidth capacity factor (default: 0.9)" },
            memoryPressure: { type: "number", description: "Host memory pressure fraction (default: 0.2)" },
            computeLoad: { type: "number", description: "Normalized compute utilization (default: 0.002)" },
          },
          required: ["nodeId"],
        },
      },
      {
        name: "get_settlement_status",
        description: "Retrieve verifiable settlement audit records and certification status for a transaction from Tordial-GS.",
        inputSchema: {
          type: "object",
          properties: {
            txId: { type: "string", description: "Unique transaction identifier to query" },
          },
          required: ["txId"],
        },
      },

      {
        name: "route_edge_burst",
        description: "Route an autonomous agent burst across heterogeneous mesh nodes using ARM64 8D state-space optimization via Tordial-GS.",
        inputSchema: {
          type: "object",
          properties: {
            budgetSats: { type: "number", description: "Budget in satoshis (default: 500)" },
            latencyMs: { type: "number", description: "Network round-trip latency in ms (default: 4.0)" },
            queueDepth: { type: "number", description: "Target node queue depth (default: 3.0)" },
            thermalHeadroom: { type: "number", description: "Thermal headroom fraction (default: 0.01)" },
            batteryReserve: { type: "number", description: "Battery capacity fraction (default: 0.02)" },
            packetLossRate: { type: "number", description: "Link packet loss rate (default: 3.5)" },
            bandwidthCapacity: { type: "number", description: "Bandwidth capacity factor (default: 0.98)" },
            memoryPressure: { type: "number", description: "Host memory pressure fraction (default: 0.2)" },
            computeLoad: { type: "number", description: "Normalized compute utilization (default: 0.002)" },
            originNode: { type: "string", description: "Origin node ID (default: SYNARA-MCP-AGENT)" },
          },
        },
      },
      {
        name: "encode_rad_hard_glyph",
        description:
          "Runs Candle-accelerated tensor math on ARM64 NEON to produce a rad-hard harmonic waveform, toroidal pi-R constant, and auto-freeze SCRP state.",
        inputSchema: {
          type: "object",
          properties: {
            terrain_data: {
              type: "array",
              items: { type: "number" },
              description: "Array of float inputs representing input terrain or intent parameters.",
            },
            use_agentic: {
              type: "boolean",
              description: "Optional flag to trigger secondary refinement pass.",
              default: false,
            },
          },
          required: ["terrain_data"],
        },
      },
      {
        name: "read_scrp_anchor",
        description:
          "Reads the current frozen SCRP state space anchor file, returning anchor ID, timestamp, intent baseline, and invariant frequency.",
        inputSchema: {
          type: "object",
          properties: {},
        },
      },
      {
        name: "validate_e8_geodesic",
        description: "Validate E8 root routing stability using Moonshot Kimi-K3 / K2.7 deep reasoning with manifold drift coordinates.",
        inputSchema: {
          type: "object",
          properties: {
            rootIndex: { type: "number", description: "E8 root index to validate (default: 12)" },
            phaseDrift: { type: "number", description: "Phase drift coordinate (default: 0.002)" },
            lyapunovExp: { type: "number", description: "Effective Lyapunov exponent (default: -6.992)" }
          }
        }
      },
    ],
  };
});

// 2. Handle tool calls
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  if (name === "validate_e8_geodesic") {
    const rootIndex = Number(args?.rootIndex ?? 12);
    const phaseDrift = Number(args?.phaseDrift ?? 0.002);
    const lyapunov = Number(args?.lyapunovExp ?? -6.992);

    return new Promise((resolvePrompt) => {
      const pyCode = `from core.mesh.reasoning_router import SovereignReasoningRouter; import json; r = SovereignReasoningRouter(); res = r.validate_e8_geodesic(${rootIndex}, ${phaseDrift}, ${lyapunov}); print(json.dumps(res))`;
      const proc = spawn("python3", ["-c", pyCode], {
        cwd: resolve(process.env.HOME || "/data/data/com.termux/files/home", "Tordial-GS"),
        env: process.env
      });

      let stdout = "";
      let stderr = "";
      proc.stdout.on("data", (chunk) => { stdout += chunk.toString(); });
      proc.stderr.on("data", (chunk) => { stderr += chunk.toString(); });

      proc.on("close", (code) => {
        if (code !== 0) {
          resolvePrompt({
            content: [{ type: "text", text: JSON.stringify({ error: stderr || "Execution failed", code }, null, 2) }],
            isError: true
          });
          return;
        }

        try {
          const parsed = JSON.parse(stdout.trim());
          resolvePrompt({
            content: [{ type: "text", text: JSON.stringify(parsed, null, 2) }]
          });
        } catch (e) {
          resolvePrompt({
            content: [{ type: "text", text: stdout.trim() }]
          });
        }
      });
    });
  }


  if (name === "encode_rad_hard_glyph") {
    const terrainData = (args?.terrain_data as number[]) || [0.1, 0.5, 0.9, 0.4];
    const useAgentic = Boolean(args?.use_agentic);

    return new Promise((resolvePrompt) => {
      client.EncodeRadHardGlyph(
        { terrain_data: terrainData, use_agentic: useAgentic },
        (err: any, response: any) => {
          if (err) {
            resolvePrompt({
              content: [
                {
                  type: "text",
                  text: JSON.stringify({ error: err.message, status: "GRPC_ERROR" }, null, 2),
                },
              ],
              isError: true,
            });
            return;
          }

          resolvePrompt({
            content: [
              {
                type: "text",
                text: JSON.stringify(
                  {
                    status: response.status,
                    coherence: response.coherence,
                    waveformChecksum: response.waveform_checksum || response.waveformChecksum,
                    toroidalPiR: response.toroidal_pi_r || response.toroidalPiR,
                    sampleCount: (response.refined_waveform || response.refinedWaveform || []).length,
                    samplePreview: (response.refined_waveform || response.refinedWaveform || []).slice(0, 8),
                    message: response.message,
                  },
                  null,
                  2
                ),
              },
            ],
          });
        }
      );
    });
  }

      if (name === "update_peer_telemetry") {
    try {
      const p = (request.params.arguments || {}) as any;
      if (!p.nodeId) throw new Error("nodeId is required");
      const telemetry = {
        latencyMs: p.latencyMs ?? 4.0,
        queueDepth: p.queueDepth ?? 1.0,
        thermalHeadroom: p.thermalHeadroom ?? 0.05,
        batteryReserve: p.batteryReserve ?? 0.95,
        packetLossRate: p.packetLossRate ?? 0.01,
        bandwidthCapacity: p.bandwidthCapacity ?? 0.9,
        memoryPressure: p.memoryPressure ?? 0.2,
        computeLoad: p.computeLoad ?? 0.002,
      };

      const res = await tordialClient.updateTelemetry(p.nodeId, telemetry);
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(res, null, 2),
          },
        ],
      };
    } catch (err: any) {
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify({ error: err.message, status: "TELEMETRY_UPDATE_FAILED" }),
          },
        ],
        isError: true,
      };
    }
  }

  if (name === "get_settlement_status") {
    try {
      const p = (request.params.arguments || {}) as any;
      if (!p.txId) throw new Error("txId is required");

      const res = await tordialClient.getSettlementStatus(p.txId);
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(res, null, 2),
          },
        ],
      };
    } catch (err: any) {
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify({ error: err.message, status: "SETTLEMENT_QUERY_FAILED" }),
          },
        ],
        isError: true,
      };
    }
  }

  if (name === "route_edge_burst") {
    try {
      const p = (request.params.arguments || {}) as any;
      const telemetry = {
        latencyMs: p.latencyMs ?? 4.0,
        queueDepth: p.queueDepth ?? 3.0,
        thermalHeadroom: p.thermalHeadroom ?? 0.01,
        batteryReserve: p.batteryReserve ?? 0.02,
        packetLossRate: p.packetLossRate ?? 3.5,
        bandwidthCapacity: p.bandwidthCapacity ?? 0.98,
        memoryPressure: p.memoryPressure ?? 0.2,
        computeLoad: p.computeLoad ?? 0.002,
      };
      const budget = p.budgetSats ?? 500;
      const origin = p.originNode ?? "SYNARA-MCP-AGENT";

      const res = await tordialClient.routeBurst(telemetry, budget, origin);
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(res, null, 2),
          },
        ],
      };
    } catch (err: any) {
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify({ error: err.message, status: "ROUTING_FAILED" }),
          },
        ],
        isError: true,
      };
    }
  }

  if (name === "read_scrp_anchor") {
    try {
      const data = await readFile(ANCHOR_PATH, "utf-8");
      return {
        content: [
          {
            type: "text",
            text: data,
          },
        ],
      };
    } catch (err: any) {
      return {
        content: [
          {
            type: "text",
            text: JSON.stringify({ error: err.message, status: "ANCHOR_NOT_FOUND" }),
          },
        ],
        isError: true,
      };
    }
  }

  throw new Error(`Tool not found: ${name}`);
});

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
}

main().catch((err) => {
  console.error("MCP Server Error:", err);
  process.exit(1);
});
