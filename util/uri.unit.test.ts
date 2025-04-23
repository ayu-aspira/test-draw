import { buildAppUriString, buildS3UriString, getS3KeyFromURI, isAppUri, parseUri } from "@/util/uri";
import { describe, expect, it } from "vitest";

describe("Workflow Util Tests", () => {
  it("Should parse the URI.", () => {
    expect(parseUri("s3://bucket/key").protocol).toBe("s3:");
    expect(parseUri("app://bucket/key").protocol).toBe("app:");
  });

  it("Should return true if the URI is an app URI", () => {
    expect(isAppUri(parseUri("app://bucket/key"))).toBe(true);
    expect(isAppUri(parseUri("s3://bucket/key"))).toBe(false);
  });

  it("Should return true if the URI is an S3 URI", () => {
    expect(isAppUri(parseUri("s3://bucket/key"))).toBe(false);
    expect(isAppUri(parseUri("app://bucket/key"))).toBe(true);
  });

  it("Should build an S3 URI string", () => {
    expect(buildS3UriString("bucket", "key")).toBe("s3://bucket/key");
    expect(buildS3UriString("bucket", "this/is/a/key")).toBe("s3://bucket/this/is/a/key");
  });

  it("Should build an app URI string", () => {
    expect(buildAppUriString("domain", { key: "value" })).toBe("app://domain?key=value");
    expect(buildAppUriString("domain", { key1: "value", key2: "value2" })).toBe("app://domain?key1=value&key2=value2");
  });

  it("Should get the URI param", () => {
    const uri = parseUri("app://domain?key=value");
    expect(uri.searchParams.get("key")).toBe("value");
  });

  it("Should return a decoded s3 key", () => {
    const uri = parseUri("s3://bucket/key%20with%20spaces");
    const s3Key = getS3KeyFromURI(uri);
    expect(s3Key).toBe("key with spaces");
  });
});
