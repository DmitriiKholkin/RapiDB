export function awsIdentityResponse(account: string, partition = "aws") {
  return {
    response: {
      statusCode: 200,
      headers: {},
      body: Buffer.from(
        `<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><GetCallerIdentityResult><Account>${account}</Account><Arn>arn:${partition}:sts::${account}:assumed-role/fixture/session</Arn></GetCallerIdentityResult></GetCallerIdentityResponse>`,
      ),
    },
  };
}
