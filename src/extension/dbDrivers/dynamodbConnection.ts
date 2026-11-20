import { addAbortListener } from "node:events";
import * as https from "node:https";
import {
  DynamoDBClient,
  type DynamoDBClientConfig,
  ListTablesCommand,
} from "@aws-sdk/client-dynamodb";
import { fromIni } from "@aws-sdk/credential-providers";
import {
  type EndpointParameterInstructionsSupplier,
  getEndpointFromInstructions,
} from "@smithy/core/endpoints";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import type { ConnectionConfig } from "../../shared/connectionConfig";
import { validateConnectionConfig } from "../../shared/connectionValidation";
import { resolveUrlRemoteTarget } from "../../shared/urlRemoteTarget";
import type {
  SshRuntimeRequest,
  SshRuntimeTransport,
} from "../services/sshRuntime";

export function buildDynamoDBClientConfig(
  config: ConnectionConfig,
): DynamoDBClientConfig {
  return {
    maxAttempts: 1,
    region: config.awsRegion || "us-east-1",
    endpoint: config.endpoint ?? config.awsEndpoint,
    credentials:
      config.awsAccessKeyId && config.awsSecretAccessKey
        ? {
            accessKeyId: config.awsAccessKeyId,
            secretAccessKey: config.awsSecretAccessKey,
            sessionToken: config.awsSessionToken,
          }
        : config.awsProfile
          ? fromIni({ profile: config.awsProfile })
          : undefined,
  };
}

type TcpForward = Extract<SshRuntimeTransport, { kind: "tcpForward" }>;

export interface DynamoDBSshEndpoint {
  protocol: string;
  request: Extract<SshRuntimeRequest, { kind: "tcpForward" }>;
  clientConfig: Pick<
    DynamoDBClientConfig,
    "credentials" | "endpoint" | "endpointProvider"
  >;
}

function validateSshEndpoint(config: ConnectionConfig, endpoint: string): void {
  // Apply the same HTTP endpoint validation to SDK defaults as to saved endpoints,
  // before Smithy normalizes away URL components such as fragments.
  if (
    !validateConnectionConfig({ ...config, endpoint }).valid ||
    !endpoint.trim() ||
    new URL(endpoint).port === "0"
  ) {
    throw new Error(
      "[RapiDB] DynamoDB over SSH requires a valid HTTP(S) endpoint.",
    );
  }
}

export async function resolveDynamoDBSshEndpoint(
  config: ConnectionConfig,
  signal?: AbortSignal,
): Promise<DynamoDBSshEndpoint> {
  signal?.throwIfAborted();
  const configuredEndpoint = config.endpoint ?? config.awsEndpoint;
  if (configuredEndpoint !== undefined) {
    validateSshEndpoint(config, configuredEndpoint);
  }

  const client = new DynamoDBClient({
    ...buildDynamoDBClientConfig(config),
    requestHandler: {
      async handle() {
        throw new Error("[RapiDB] Endpoint resolution cannot send requests.");
      },
    },
  });
  let abortSubscription: ReturnType<typeof addAbortListener> | undefined;
  try {
    // Use the same SDK endpoint middleware inputs as the driver's connection
    // probe. This resolves settings and credential metadata, but sends no
    // DynamoDB operation and does not perform DynamoDB endpoint discovery.
    const resolution = getEndpointFromInstructions(
      { Limit: 1 },
      // The generated Command type exposes the SDK's own instructions as unknown.
      ListTablesCommand as EndpointParameterInstructionsSupplier,
      {
        ...client.config,
        serviceConfiguredEndpoint: async () => {
          const value = await client.config.serviceConfiguredEndpoint?.();
          if (value) validateSshEndpoint(config, value);
          return value;
        },
      },
    );
    // Credential providers do not accept an AbortSignal, but a cancelled lookup
    // must not hold up connection cancellation or open a tunnel when it returns.
    const endpoint = signal
      ? await Promise.race([
          resolution,
          new Promise<never>((_resolve, reject) => {
            abortSubscription = addAbortListener(signal, () =>
              reject(signal.reason),
            );
          }),
        ])
      : await resolution;
    validateSshEndpoint(config, endpoint.url.href);
    const target = resolveUrlRemoteTarget(endpoint.url.href, 443);
    if (!target) {
      throw new Error("[RapiDB] No DynamoDB endpoint was resolved for SSH.");
    }

    return {
      protocol: endpoint.url.protocol,
      request: {
        kind: "tcpForward",
        remoteHost: target.host,
        remotePort: target.port,
      },
      clientConfig: {
        credentials: client.config.credentials,
        // Prevent environment changes or operation-specific endpoint rules from
        // selecting a different destination. Keep the SDK's signing properties.
        endpoint: endpoint.url.href,
        endpointProvider: () => endpoint,
      },
    };
  } finally {
    abortSubscription?.[Symbol.dispose]();
    client.destroy();
  }
}

export class DynamoDBSshRequestHandler extends NodeHttpHandler {
  constructor(
    private readonly endpoint: DynamoDBSshEndpoint,
    private readonly transport: TcpForward,
    servername?: string,
  ) {
    super({
      httpsAgent: new https.Agent({
        keepAlive: true,
        servername: servername ?? endpoint.request.remoteHost,
      }),
    });
  }

  override async handle(
    request: Parameters<NodeHttpHandler["handle"]>[0],
    options?: Parameters<NodeHttpHandler["handle"]>[1],
  ): ReturnType<NodeHttpHandler["handle"]> {
    const { remoteHost, remotePort } = this.endpoint.request;
    const port = request.port ?? (request.protocol === "https:" ? 443 : 80);
    if (
      request.protocol !== this.endpoint.protocol ||
      request.hostname.replace(/^\[|\]$/g, "") !== remoteHost ||
      port !== remotePort ||
      this.transport.remoteHost !== remoteHost ||
      this.transport.remotePort !== remotePort
    ) {
      throw new Error(
        "[RapiDB] DynamoDB request does not match the fixed SSH target.",
      );
    }

    // Only replace the socket address, after signing. Keep the HTTP Host,
    // request path, and other signed fields unchanged.
    const localRequest = request.clone();
    localRequest.hostname = this.transport.localHost;
    localRequest.port = this.transport.localPort;
    return super.handle(localRequest, options);
  }
}
