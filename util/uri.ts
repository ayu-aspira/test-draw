export const parseUri = (uri: string): URL => new URL(uri);
export const isAppUri = (uri: URL): boolean => uri.protocol === "app:";
export const isS3Uri = (uri: URL): boolean => uri.protocol === "s3:";

export const getS3KeyFromURI = (uri: URL): string => decodeURI(uri.pathname.substring(1));
export const buildS3UriString = (bucket: string, key: string): string => `s3://${bucket}/${key}`;

export const buildAppUriString = (domain: string, params: { [k in string]: string }, path?: string): string => {
  const uri = new URL(`app://${domain.toLowerCase()}`);

  for (const [k, v] of Object.entries(params)) {
    uri.searchParams.set(k, v);
  }

  if (path) {
    uri.pathname = path;
  }

  return uri.toString();
};

export const getUriParam = (uri: URL, paramKey: string): string | null => uri.searchParams.get(paramKey);
