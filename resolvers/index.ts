import { logger, tracer } from "@/logger/logger";
import { mutations } from "@/resolvers/mutations";
import { batchQueries, queries } from "@/resolvers/queries";
import { createResolverHandler } from "@aspira-nextgen/graphql/resolver-handler";

export const handler = createResolverHandler([mutations, queries], logger, tracer);
export const batchHandler = createResolverHandler([batchQueries], logger, tracer);
