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
    ],
  };
});

// 2. Handle tool calls
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

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
