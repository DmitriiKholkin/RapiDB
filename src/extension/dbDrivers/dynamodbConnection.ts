import { addAbortListener } from "node:events";
import * as https from "node:https";
import {
  DynamoDBClient,
  type DynamoDBClientConfig,
  ListTablesCommand,
} from "@aws-sdk/client-dynamodb";
import { fromIni, fromNodeProviderChain } from "@aws-sdk/credential-providers";
import {
  type EndpointParameterInstructionsSupplier,
  getEndpointFromInstructions,
} from "@smithy/core/endpoints";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import {
  type ConnectionConfig,
  resolveAwsCredentialMode,
} from "../../shared/connectionConfig";
import { hasAmbiguousUriCredentials } from "../../shared/connectionUriSecurity";
import { resolveUrlRemoteTarget } from "../../shared/urlRemoteTarget";
import type {
  SshRuntimeRequest,
  SshRuntimeTransport,
} from "../services/sshRuntime";
import {
  type DynamoCredentials,
  DynamoDBTarget,
  DynamoDBTargetChangedError,
} from "./dynamodbTarget";

export function buildDynamoDBClientConfig(
  config: ConnectionConfig,
): DynamoDBClientConfig {
  const mode = resolveAwsCredentialMode(config);
  const region = config.awsRegion || "us-east-1";
  let credentials: DynamoDBClientConfig["credentials"];
  switch (mode) {
    case "static": {
      const accessKeyId = config.awsAccessKeyId?.trim();
      const secretAccessKey = config.awsSecretAccessKey?.trim();
      const sessionToken = config.awsSessionToken?.trim() || undefined;
      if (
        !accessKeyId ||
        !secretAccessKey ||
        (!sessionToken &&
          (config.awsSessionTokenRequired || accessKeyId.startsWith("ASIA")))
      ) {
        throw new Error(
          "[RapiDB] Static AWS credentials are incomplete. Re-enter the access key, secret key and, if applicable, session token in Edit Connection.",
        );
      }
      credentials = { accessKeyId, secretAccessKey, sessionToken };
      break;
    }
    case "profile": {
      if (!config.awsProfile?.trim())
        throw new Error(
          "[RapiDB] Enter a named AWS profile in Edit Connection.",
        );
      credentials = fromIni({
        profile: config.awsProfile.trim(),
        ignoreCache: true,
        parentClientConfig: { region: async () => region },
      });
      break;
    }
    case "default":
      credentials = fromNodeProviderChain({
        ignoreCache: true,
        parentClientConfig: { region: async () => region },
      });
      break;
    default:
      throw new Error(
        "[RapiDB] The DynamoDB credential source is unknown. Open Edit Connection and explicitly select a credential source or restore the saved access keys.",
      );
  }
  return {
    maxAttempts: 1,
    region,
    endpoint: config.endpoint ?? config.awsEndpoint,
    credentials,
  };
}

type TcpForward = Extract<SshRuntimeTransport, { kind: "tcpForward" }>;

export interface ResolvedDynamoDBEndpoint {
  url: URL;
  target: DynamoDBTarget;
  clientConfig: Pick<
    DynamoDBClientConfig,
    "region" | "endpoint" | "endpointProvider"
  >;
}

export interface DynamoDBSshEndpoint extends ResolvedDynamoDBEndpoint {
  protocol: string;
  request: Extract<SshRuntimeRequest, { kind: "tcpForward" }>;
}

function validateEndpoint(endpoint: string): void {
  // Validate SDK overrides too, before Smithy normalizes away fragments.
  try {
    const url = new URL(endpoint);
    if (
      (url.protocol === "http:" || url.protocol === "https:") &&
      url.hostname &&
      url.port !== "0" &&
      !url.hash &&
      !hasAmbiguousUriCredentials(endpoint)
    )
      return;
  } catch {}
  throw new Error("[RapiDB] DynamoDB requires a valid HTTP(S) endpoint.");
}

export async function resolveDynamoDBEndpoint(
  config: ConnectionConfig,
  signal: AbortSignal,
): Promise<ResolvedDynamoDBEndpoint> {
  signal.throwIfAborted();
  const configuredEndpoint = config.endpoint ?? config.awsEndpoint;
  if (configuredEndpoint !== undefined) {
    validateEndpoint(configuredEndpoint);
  }

  const configInput: DynamoDBClientConfig = {
    ...buildDynamoDBClientConfig(config),
    requestHandler: {
      async handle() {
        throw new Error("[RapiDB] Endpoint resolution cannot send requests.");
      },
    },
  };
  const credentialsProvider = configInput.credentials;
  const loadCredentials = async (forceRefresh: boolean) => {
    if (!credentialsProvider)
      throw new Error("[RapiDB] Missing DynamoDB credential provider.");
    return typeof credentialsProvider === "function"
      ? credentialsProvider({ forceRefresh })
      : credentialsProvider;
  };
  const client = new DynamoDBClient(configInput);
  let abortSubscription: ReturnType<typeof addAbortListener> | undefined;
  const resolve = async (
    probe: DynamoDBClient,
    credentials: Pick<DynamoCredentials, "accountId" | "credentialScope">,
  ) => {
    const endpoint = await getEndpointFromInstructions(
      { Limit: 1 },
      // The generated Command type exposes the SDK's own instructions as unknown.
      ListTablesCommand as EndpointParameterInstructionsSupplier,
      {
        ...probe.config,
        credentials: async () => credentials,
        serviceConfiguredEndpoint: async () => {
          const value = await probe.config.serviceConfiguredEndpoint?.();
          if (value) validateEndpoint(value);
          return value;
        },
      },
    );
    validateEndpoint(endpoint.url.href);
    const region = await probe.config.region();
    const auth = endpoint.properties?.authSchemes;
    const signingRegion =
      Array.isArray(auth) && typeof auth[0]?.signingRegion === "string"
        ? auth[0].signingRegion
        : region;
    return { endpoint, region, signingRegion };
  };
  try {
    const { endpoint, region, signingRegion, initialCredentials } =
      await Promise.race([
        (async () => {
          const initialCredentials = await loadCredentials(false);
          return {
            ...(await resolve(client, initialCredentials)),
            initialCredentials,
          };
        })(),
        new Promise<never>((_resolve, reject) => {
          abortSubscription = addAbortListener(signal, () =>
            reject(signal.reason),
          );
        }),
      ]);
    signal.throwIfAborted();

    // Freeze only the routing metadata, not the original access keys. The
    // verifier proves the account of refreshed keys independently of metadata.
    const routingMetadata = {
      accountId: initialCredentials.accountId,
      credentialScope: initialCredentials.credentialScope,
    };
    return {
      url: endpoint.url,
      target: new DynamoDBTarget(
        endpoint.url,
        signingRegion,
        initialCredentials,
        async () => {
          const credentials = await loadCredentials(true);
          // Resolve SDK settings again on explicit verification/refresh. Never
          // use credentials for a newly resolved endpoint on the old connection.
          const probe = new DynamoDBClient(configInput);
          try {
            const next = await resolve(probe, routingMetadata);
            if (
              next.endpoint.url.href !== endpoint.url.href ||
              next.signingRegion !== signingRegion
            ) {
              throw new DynamoDBTargetChangedError();
            }
            return credentials;
          } finally {
            probe.destroy();
          }
        },
        config.awsStsEndpoint,
      ),
      clientConfig: {
        region,
        endpoint: endpoint.url.href,
        endpointProvider: () => endpoint,
      },
    };
  } finally {
    abortSubscription?.[Symbol.dispose]();
    client.destroy();
  }
}

export async function resolveDynamoDBSshEndpoint(
  config: ConnectionConfig,
  signal: AbortSignal = new AbortController().signal,
): Promise<DynamoDBSshEndpoint> {
  const resolved = await resolveDynamoDBEndpoint(config, signal);
  const endpoint = resolved.url;
  const target = resolveUrlRemoteTarget(endpoint.href, 443);
  if (!target)
    throw new Error("[RapiDB] No DynamoDB endpoint was resolved for SSH.");
  const stsHostname = resolved.target.stsHostname;
  return {
    ...resolved,
    protocol: endpoint.protocol,
    request: {
      kind: "tcpForward",
      remoteHost: target.host,
      remotePort: target.port,
      // AWS account verification must use the same network path as DynamoDB.
      ...(stsHostname
        ? { extraForwards: [{ remoteHost: stsHostname, remotePort: 443 }] }
        : {}),
    },
  };
}

// Binds account verification of an AWS target to the SSH forward to STS.
export function routeDynamoDBStsThroughSsh(
  endpoint: DynamoDBSshEndpoint,
  transport: TcpForward,
): void {
  const stsHostname = endpoint.target.stsHostname;
  if (!stsHostname) return;
  const forward = transport.extraForwards?.find(
    (candidate) =>
      candidate.remoteHost === stsHostname && candidate.remotePort === 443,
  );
  if (!forward) {
    throw new Error(
      `[RapiDB] DynamoDB over SSH needs a forward to ${stsHostname}:443 to verify the AWS account; none was established.`,
    );
  }
  endpoint.target.routeStsThrough({
    localHost: forward.localHost,
    localPort: forward.localPort,
  });
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
        ...(endpoint.target.isAws ? { rejectUnauthorized: true } : {}),
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
