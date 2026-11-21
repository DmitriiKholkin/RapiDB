import { createHash } from "node:crypto";
import { addAbortListener } from "node:events";
import * as https from "node:https";
import type { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { partition } from "@aws-sdk/core/client";
import { parseXML } from "@aws-sdk/xml-builder";
import { HttpRequest } from "@smithy/core/protocols";
import { Hash } from "@smithy/core/serde";
import { NodeHttpHandler } from "@smithy/node-http-handler";
import { SignatureV4 } from "@smithy/signature-v4";

export type DynamoCredentials = Awaited<
  ReturnType<DynamoDBClient["config"]["credentials"]>
>;

interface AwsEndpoint {
  region: string;
  partition: string;
  dnsSuffix: string;
  dualStackDnsSuffix: string;
  account?: string;
}

export const DYNAMODB_TARGET_CHANGED =
  "[RapiDB] DynamoDB credentials now resolve to a different target. Reconnect and explicitly select the intended target before running saved work.";

export class DynamoDBTargetChangedError extends Error {
  constructor() {
    super(DYNAMODB_TARGET_CHANGED);
    this.name = "DynamoDBTargetChangedError";
  }
}

function classifyAwsEndpoint(
  endpoint: URL,
  signingRegion: string,
): AwsEndpoint | undefined {
  const host = endpoint.hostname.replace(/\.$/, "");
  // Match service names and the partition's SDK DNS suffix, not arbitrary
  // amazonaws.com hosts or custom aliases. Include account and VPC endpoints.
  const match =
    /^(?:dynamodb(?:-fips)?\.|(\d{12})\.ddb(?:-fips)?\.|vpce-[a-z0-9-]+\.dynamodb(?:-fips)?\.)([a-z0-9-]+)\.(.+)$/.exec(
      host,
    );
  if (!match) return;
  const [, account, region, suffix] = match;
  const aws = partition(region);
  if (
    suffix !== aws.dnsSuffix &&
    suffix !== aws.dualStackDnsSuffix &&
    suffix !== `vpce.${aws.dnsSuffix}`
  )
    return;
  if (
    region !== signingRegion ||
    !/^[a-z][a-z0-9]*(?:-[a-z0-9]+)+-\d+$/.test(region) ||
    endpoint.protocol !== "https:" ||
    endpoint.port ||
    endpoint.username ||
    endpoint.password ||
    endpoint.pathname !== "/" ||
    endpoint.search
  ) {
    throw new Error(
      "[RapiDB] The AWS DynamoDB endpoint must use HTTPS and match the signing region.",
    );
  }
  return {
    region,
    account,
    partition: aws.name,
    dnsSuffix: aws.dnsSuffix,
    dualStackDnsSuffix: aws.dualStackDnsSuffix,
  };
}

const STS_ENDPOINT_FIELD = "STS endpoint (awsStsEndpoint)";

// An explicit STS endpoint replaces only the address of account verification.
// It must be an AWS STS name for the DynamoDB region (regional, FIPS,
// dual-stack or an interface VPC endpoint's DNS name): the signed request
// and the account it reports must come from AWS, over verified TLS.
function parseAwsStsEndpoint(value: string, aws: AwsEndpoint): URL {
  const trimmed = value.trim();
  let url: URL | undefined;
  try {
    url = new URL(trimmed);
  } catch {}
  const host = url?.hostname.replace(/\.$/, "").toLowerCase();
  const escapeRegExp = (text: string) =>
    text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const region = escapeRegExp(aws.region);
  const suffix = escapeRegExp(aws.dnsSuffix);
  const dualStack = escapeRegExp(aws.dualStackDnsSuffix);
  const sts = new RegExp(
    `^(?:sts(?:-fips)?\\.${region}\\.(?:${suffix}|${dualStack})|vpce-[a-z0-9-]+\\.sts(?:-fips)?\\.${region}\\.vpce\\.${suffix})$`,
  );
  if (
    !url ||
    !host ||
    url.protocol !== "https:" ||
    url.username ||
    url.password ||
    url.port ||
    url.pathname !== "/" ||
    url.search ||
    url.hash ||
    /[?#@\s]/.test(trimmed) ||
    !sts.test(host)
  ) {
    throw new Error(
      `[RapiDB] The ${STS_ENDPOINT_FIELD} must be an HTTPS AWS STS endpoint for region ${aws.region} without a port, path, query or credentials, for example https://sts.${aws.region}.${aws.dnsSuffix} or the DNS name of an STS interface VPC endpoint (https://vpce-….sts.${aws.region}.vpce.${aws.dnsSuffix}).`,
    );
  }
  return new URL(`https://${host}/`);
}

/** A local SSH forward to the regional STS endpoint (port 443). */
export interface DynamoDBStsRoute {
  localHost: string;
  localPort: number;
}

export function awsStsHostname(aws: {
  region: string;
  dnsSuffix: string;
}): string {
  return `sts.${aws.region}.${aws.dnsSuffix}`;
}

async function getAwsAccount(
  aws: AwsEndpoint,
  hostname: string,
  custom: boolean,
  credentials: DynamoCredentials,
  signal: AbortSignal,
  route?: DynamoDBStsRoute,
): Promise<string> {
  // Do not inherit AWS_ENDPOINT_URL[_STS] or shared AWS endpoint overrides;
  // only the profile's explicit STS endpoint replaces regional STS. Only a
  // classified AWS target reaches here. Use AWS Query protocol with the SDK's
  // SigV4 signer (service sts, the DynamoDB region) and XML parser. With SSH,
  // connect through the same SSH session (fixed forward to the STS host:443);
  // TLS SNI, certificate verification and the signed Host header still use
  // the STS hostname.
  const handler = new NodeHttpHandler({
    connectionTimeout: 5_000,
    requestTimeout: 10_000,
    httpsAgent: route
      ? new https.Agent({ servername: hostname, rejectUnauthorized: true })
      : { rejectUnauthorized: true },
  });
  const abort = addAbortListener(signal, () => handler.destroy());
  try {
    const request = await new SignatureV4({
      credentials,
      region: aws.region,
      service: "sts",
      sha256: Hash.bind(null, "sha256"),
    }).sign(
      new HttpRequest({
        protocol: "https:",
        hostname,
        method: "POST",
        path: "/",
        headers: {
          host: hostname,
          "content-type": "application/x-www-form-urlencoded; charset=utf-8",
        },
        body: "Action=GetCallerIdentity&Version=2011-06-15",
      }),
    );
    signal.throwIfAborted();
    const outgoing = new HttpRequest(request);
    if (route) {
      // Only the socket address changes after signing.
      outgoing.hostname = route.localHost;
      outgoing.port = route.localPort;
    }
    const { response } = await handler.handle(outgoing, {
      abortSignal: signal,
    });
    if (response.statusCode !== 200) throw new Error();
    const chunks: Buffer[] = [];
    let length = 0;
    const body = Buffer.isBuffer(response.body)
      ? [response.body]
      : response.body;
    for await (const chunk of body) {
      signal.throwIfAborted();
      const bytes = Buffer.from(chunk);
      length += bytes.length;
      if (length > 16_384) throw new Error();
      chunks.push(bytes);
    }
    const xml = Buffer.concat(chunks).toString("utf8");
    // AWS returns neither DTDs nor entity definitions in this response.
    if (/<!DOCTYPE|<!ENTITY/i.test(xml)) throw new Error();
    const result: { Account?: unknown; Arn?: unknown } | undefined =
      parseXML(xml)?.GetCallerIdentityResponse?.GetCallerIdentityResult;
    const account = result?.Account;
    const arn = typeof result?.Arn === "string" ? result.Arn.split(":") : [];
    if (
      typeof account !== "string" ||
      !/^\d{12}$/.test(account) ||
      arn[0] !== "arn" ||
      arn[1] !== aws.partition ||
      (arn[2] !== "iam" && arn[2] !== "sts") ||
      arn[4] !== account ||
      !arn[5]
    )
      throw new Error();
    return account;
  } catch {
    signal.throwIfAborted();
    const via = custom ? "the configured STS endpoint" : "regional STS";
    const alternative = custom
      ? `Check the ${STS_ENDPOINT_FIELD} setting`
      : `Without private DNS on an STS interface VPC endpoint, set its DNS name as the ${STS_ENDPOINT_FIELD} in Edit Connection`;
    throw new Error(
      route
        ? `[RapiDB] Could not verify the AWS DynamoDB account through ${via} (${hostname}) via the SSH host. The SSH host must be able to reach ${hostname}:443 — for a private subnet without internet access, add an STS interface VPC endpoint. ${alternative}. Also check the credentials.`
        : `[RapiDB] Could not verify the AWS DynamoDB account through ${via} (${hostname}). Check the credentials and STS connectivity. ${alternative}.`,
    );
  } finally {
    abort[Symbol.dispose]();
    handler.destroy();
  }
}

const digest = (value: unknown): string =>
  createHash("sha256").update(JSON.stringify(value)).digest("hex");

export class DynamoDBTarget {
  private readonly aws: AwsEndpoint | undefined;
  private readonly lifetime = new AbortController();
  private verified?: {
    credentials: DynamoCredentials;
    credentialKey: string;
    fingerprint: string;
  };
  private checking?: Promise<DynamoCredentials>;
  private changed = false;
  private initialCredentials?: DynamoCredentials;
  private stsRoute?: DynamoDBStsRoute;
  private readonly stsEndpoint: URL | undefined;

  constructor(
    private readonly endpoint: URL,
    private readonly signingRegion: string,
    initialCredentials: DynamoCredentials,
    private readonly resolveCredentials: () => Promise<DynamoCredentials>,
    stsEndpoint?: string,
  ) {
    this.aws = classifyAwsEndpoint(endpoint, signingRegion);
    if (stsEndpoint?.trim()) {
      if (!this.aws) {
        throw new Error(
          `[RapiDB] The ${STS_ENDPOINT_FIELD} applies only to recognized AWS DynamoDB endpoints. Clear it for DynamoDB Local or custom endpoints.`,
        );
      }
      this.stsEndpoint = parseAwsStsEndpoint(stsEndpoint, this.aws);
    }
    this.initialCredentials = initialCredentials;
  }

  get fingerprint(): string | undefined {
    return this.changed ? undefined : this.verified?.fingerprint;
  }

  get isAws(): boolean {
    return this.aws !== undefined;
  }

  /** The STS host that must be reachable to verify an AWS target. */
  get stsHostname(): string | undefined {
    return this.aws && (this.stsEndpoint?.hostname ?? awsStsHostname(this.aws));
  }

  /** Send account verification through a fixed SSH forward to STS. */
  routeStsThrough(route: DynamoDBStsRoute): void {
    this.stsRoute = route;
  }

  // Disable the SDK's additional credential memoization for this provider.
  // Every signing attempt must use a snapshot accepted by this target guard.
  readonly credentials = Object.assign(
    async () => {
      this.lifetime.signal.throwIfAborted();
      if (this.changed) throw new DynamoDBTargetChangedError();
      const current = this.verified?.credentials;
      return (
        this.checking ??
        (current &&
        (!current.expiration ||
          current.expiration.getTime() > Date.now() + 60_000)
          ? current
          : this.verify())
      );
    },
    { memoized: true },
  );

  verify(): Promise<DynamoCredentials> {
    this.lifetime.signal.throwIfAborted();
    if (this.changed) return Promise.reject(new DynamoDBTargetChangedError());
    this.checking ??= this.accept(this.resolveCredentials)
      .catch((error: unknown) => {
        if (error instanceof DynamoDBTargetChangedError) this.changed = true;
        throw error;
      })
      .finally(() => {
        this.checking = undefined;
      });
    return this.checking;
  }

  async initialize(): Promise<void> {
    const credentials = this.initialCredentials;
    this.initialCredentials = undefined;
    if (!credentials)
      throw new Error("[RapiDB] Initial DynamoDB credentials are unavailable.");
    await this.accept(() => Promise.resolve(credentials));
  }

  dispose(): void {
    this.lifetime.abort();
    this.initialCredentials = undefined;
    this.verified = undefined;
  }

  private async accept(
    resolve: () => Promise<DynamoCredentials>,
  ): Promise<DynamoCredentials> {
    const signal = AbortSignal.any([
      this.lifetime.signal,
      AbortSignal.timeout(10_000),
    ]);
    let subscription: ReturnType<typeof addAbortListener> | undefined;
    let credentials: DynamoCredentials;
    try {
      credentials = {
        ...(await Promise.race([
          resolve(),
          new Promise<never>((_resolve, reject) => {
            subscription = addAbortListener(signal, () =>
              reject(signal.reason),
            );
          }),
        ])),
      };
    } finally {
      subscription?.[Symbol.dispose]();
    }
    signal.throwIfAborted();
    if (
      credentials.expiration &&
      credentials.expiration.getTime() <= Date.now()
    ) {
      throw new Error("[RapiDB] DynamoDB credentials have expired.");
    }
    const credentialKey = digest([
      credentials.accessKeyId,
      credentials.secretAccessKey,
      credentials.sessionToken,
    ]);
    let fingerprint =
      this.verified?.credentialKey === credentialKey
        ? this.verified.fingerprint
        : undefined;
    if (!fingerprint) {
      const account =
        this.aws &&
        (await getAwsAccount(
          this.aws,
          this.stsHostname as string,
          this.stsEndpoint !== undefined,
          credentials,
          signal,
          this.stsRoute,
        ));
      fingerprint = digest([
        "dynamodb-target-v1",
        this.endpoint.href,
        this.signingRegion,
        this.aws
          ? [
              "aws",
              this.aws.partition,
              account,
              // The verifying endpoint is part of the binding when explicit.
              ...(this.stsEndpoint ? ["sts", this.stsEndpoint.href] : []),
            ]
          : // Temporary credentials (with a session token) rotate their
            // access key ID on every refresh; binding to it would break the
            // session. For them the custom binding is endpoint + region only.
            credentials.sessionToken
            ? ["custom", "temporary-credentials"]
            : ["custom", credentials.accessKeyId],
      ]);
      if (
        (this.aws?.account && this.aws.account !== account) ||
        (this.verified && this.verified.fingerprint !== fingerprint)
      ) {
        this.changed = true;
        throw new DynamoDBTargetChangedError();
      }
    }
    signal.throwIfAborted();
    this.verified = { credentials, credentialKey, fingerprint };
    return credentials;
  }
}
