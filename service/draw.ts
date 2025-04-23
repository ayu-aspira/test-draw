import {
  type DrawEntity,
  createDrawEntity,
  deleteDrawEntity,
  getDrawEntity,
  listDrawEntitiesForDrawCategory,
  updateDrawEntity,
} from "@/dynamo/draw";
import { DynamoDBTypename } from "@/dynamo/dynamo";
import { ApplicantSortOptions } from "@/s3/applicant";
import { getDrawDocuments } from "@/service/draw-document";
import { GraphQLTypename } from "@/service/graphql";
import type { Token } from "@aspira-nextgen/core/authn";
import { addTypename, buildCreateMetadata } from "@aspira-nextgen/core/dynamodb";
import {
  type CreateDrawInput,
  type DeleteDrawInput,
  type Draw,
  type DrawCategory,
  type DrawConnection,
  type DrawDocument,
  DrawDocumentType,
  DrawSortDirection,
  type ListDrawsInput,
  type UpdateDrawInput,
} from "@aspira-nextgen/graphql/resolvers";
import { z } from "zod";

export const createDraw = async (input: CreateDrawInput, token: Token): Promise<Draw> => {
  const organizationId = token.claims.organizationId;

  const drawEntity = await createDrawEntity(
    {
      ...input,
      ...buildCreateMetadata(token.sub),
    },
    token,
  );

  const { drawDataDocument, applicantDocument } = await fetchDrawDocuments(drawEntity, organizationId);

  return {
    ...drawEntity,
    drawDataDocument: drawDataDocument,
    applicantDocument: applicantDocument,
    sortRules: [], // Draws are created w/out sort rules.
    drawCategoryId: input.drawCategoryId,
    __typename: DynamoDBTypename.DRAW,
  };
};

const UpdateDrawInputSchema = z.object({
  id: z.string(),
  name: z.string().optional(),
  applicantDocumentId: z.string().optional(),
  drawDataDocumentId: z.string().optional(),
  drawCategoryId: z.string().optional(),
  sortRules: z.array(z.object({ field: z.string(), direction: z.nativeEnum(DrawSortDirection) })).optional(),
});

export const updateDraw = async (input: UpdateDrawInput, token: Token): Promise<Draw> => {
  const organizationId = token.claims.organizationId;

  const validation = UpdateDrawInputSchema.safeParse(input);
  if (!validation.success) {
    throw new Error(`Invalid input: ${JSON.stringify(validation.error.flatten())}`, { cause: validation.error });
  }

  const { sortRules } = input;

  if (sortRules) {
    if (sortRules.some((rule) => !ApplicantSortOptions.find((option) => option.field === rule.field))) {
      throw new Error("Invalid sort rule.");
    }

    if (new Set(input.sortRules?.map((rule) => rule.field)).size !== input.sortRules?.length) {
      throw new Error("Sorting multiple times on the same field is not allowed.");
    }
  }

  const drawEntity = await updateDrawEntity({
    id: {
      organizationId,
      drawId: input.id,
    },
    // Zod .optional() disallows explicit nulls, so this cast is fine.
    updates: validation.data as z.infer<typeof UpdateDrawInputSchema>,
    updatedBy: token.sub,
  });

  const { drawDataDocument, applicantDocument } = await fetchDrawDocuments(drawEntity, organizationId);

  return {
    ...drawEntity,
    drawDataDocument,
    applicantDocument,
    sortRules: addTypename(GraphQLTypename.DRAW_SORT_RULE, drawEntity.sortRules),
    __typename: DynamoDBTypename.DRAW,
  };
};

export const deleteDraw = async (input: DeleteDrawInput, token: Token): Promise<boolean> => {
  const { id } = input;

  await deleteDrawEntity({ drawId: id }, token);

  return true;
};

export const getDraw = async (id: string, token: Token): Promise<Draw> => {
  const organizationId = token.claims.organizationId;

  const drawEntity = await getDrawEntity({ drawId: id, organizationId });

  const { drawDataDocument, applicantDocument } = await fetchDrawDocuments(drawEntity, organizationId);

  return {
    ...drawEntity,
    drawDataDocument,
    applicantDocument,
    sortRules: addTypename(GraphQLTypename.DRAW_SORT_RULE, drawEntity.sortRules),
    __typename: DynamoDBTypename.DRAW,
  };
};

export const batchListDrawsForDrawCategories = async (
  input: {
    drawCategory: DrawCategory;
    listDrawsInput: ListDrawsInput;
  }[],
  token: Token,
): Promise<DrawConnection[]> => {
  return await Promise.all(
    input.map(({ drawCategory, listDrawsInput }) =>
      listDrawsForDrawCategory({ drawCategoryId: drawCategory.id, listDrawsInput }, token),
    ),
  );
};

export const listDrawsForDrawCategory = async (
  input: {
    drawCategoryId: string;
    listDrawsInput: ListDrawsInput;
  },
  token: Token,
): Promise<DrawConnection> => {
  const { drawCategoryId, listDrawsInput } = input;
  const { organizationId } = token.claims;

  const { items: drawEntities, nextToken } = await listDrawEntitiesForDrawCategory({
    drawCategoryId,
    organizationId,
    paginationRequest: listDrawsInput,
  });

  const docIds = drawEntities.flatMap((draw) => [draw.drawDataDocumentId, draw.applicantDocumentId]);

  const documents = (await getDrawDocuments(docIds, organizationId)).reduce(
    (acc, doc) => {
      acc[doc.id] = doc;
      return acc;
    },
    {} as Record<string, DrawDocument>,
  );

  const draws: Draw[] = drawEntities.map((draw) => {
    const drawDataDocument = documents[draw.drawDataDocumentId];
    const applicantDocument = documents[draw.applicantDocumentId];

    if (!drawDataDocument || !applicantDocument) {
      throw new Error("Document not found.");
    }

    return {
      ...draw,
      drawDataDocument,
      applicantDocument,
      sortRules: addTypename(GraphQLTypename.DRAW_SORT_RULE, draw.sortRules),
      __typename: DynamoDBTypename.DRAW,
    };
  });

  return {
    items: draws,
    nextToken,
    __typename: GraphQLTypename.DRAW_CONNECTION,
  };
};

const fetchDrawDocuments = async (
  draw: DrawEntity,
  organizationId: string,
): Promise<{
  drawDataDocument: DrawDocument;
  applicantDocument: DrawDocument;
}> => {
  const { drawDataDocumentId, applicantDocumentId } = draw;

  const drawDocuments = await getDrawDocuments([drawDataDocumentId, applicantDocumentId], organizationId);

  const drawDataDocument = drawDocuments.find((doc) => doc.type === DrawDocumentType.HuntCodes);
  const applicantDocument = drawDocuments.find((doc) => doc.type === DrawDocumentType.Applicants);

  // Shouldn't hit this, but just in case.
  if (!drawDataDocument || !applicantDocument) {
    throw new Error("Document not found.");
  }

  return {
    drawDataDocument,
    applicantDocument,
  };
};
