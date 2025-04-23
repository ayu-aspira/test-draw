import { getDrawConfigEntity } from "@/dynamo/draw-config";
import { getDrawTableName } from "@/dynamo/dynamo";
import { dynamoDBClient } from "@/dynamo/dynamo";
import {
  createDrawConfig,
  deleteDrawConfig,
  deleteDrawConfigForWorkflowNode,
  getDrawConfig,
  updateDrawConfig,
} from "@/service/draw-config";
import { createTestDrawSort, deleteAllS3TestDataByOrg } from "@/util/test-utils";
import { buildTestToken } from "@aspira-nextgen/core/authn-test-utils";
import { deleteAllTableItemsByPk } from "@aspira-nextgen/core/dynamodb-test-utils";
import {
  type CreateDrawConfigInput,
  DrawConfigQuotaRule,
  type UpdateDrawConfigInput,
} from "@aspira-nextgen/graphql/resolvers";
import { ulid } from "ulidx";
import { afterEach, describe, expect, it } from "vitest";

const ORG_ID = `org_${ulid()}`;
const TOKEN = buildTestToken(ORG_ID);

const createTestDrawConfig = async (opts?: { workflowNodeId?: string }) => {
  const { workflowNodeId = "node_1" } = opts || {};
  const drawSort = await createTestDrawSort({}, TOKEN);

  const input: CreateDrawConfigInput = {
    workflowNodeId,
    name: `Test Draw Config ${ulid()}`,
    sortId: drawSort.id,
    usePoints: Math.random() > 0.4999999999,
    quotaRules: {
      ruleFlags: [DrawConfigQuotaRule.FlowQuota, DrawConfigQuotaRule.NonResidentCapEnforcement],
    },
  };

  return await createDrawConfig(input, TOKEN);
};

describe("Draw Config Service Tests", () => {
  afterEach(async () => {
    await deleteAllTableItemsByPk(dynamoDBClient, getDrawTableName(), [ORG_ID]);
    await deleteAllS3TestDataByOrg(ORG_ID);
  });

  describe("Create Draw Config", () => {
    it("Should create a config", async () => {
      const drawSort = await createTestDrawSort({}, TOKEN);

      const input: CreateDrawConfigInput = {
        workflowNodeId: "node_1",
        name: `Test Draw Config ${ulid()}`,
        sortId: drawSort.id,
        usePoints: Math.random() > 0.4999999999,
        quotaRules: {
          ruleFlags: [DrawConfigQuotaRule.FlowQuota, DrawConfigQuotaRule.NonResidentCapEnforcement],
        },
      };

      const { id: drawConfigId } = await createDrawConfig(input, TOKEN);

      const config = await getDrawConfigEntity({ drawConfigId, organizationId: ORG_ID });
      expect(config).toMatchObject({
        id: expect.stringMatching(/^draw_config_/),
        name: input.name,
        sortId: input.sortId,
        usePoints: input.usePoints,
        quotaRuleFlags: input.quotaRules.ruleFlags,
      });
    });
  });

  it("Should fail to create if sort is not found", async () => {
    const sortId = `sort_${ulid()}`;
    const input: CreateDrawConfigInput = {
      workflowNodeId: "node_1",
      name: `Test Draw Config ${ulid()}`,
      sortId,
      usePoints: Math.random() > 0.4999999999,
      quotaRules: {
        ruleFlags: [],
      },
    };

    await expect(createDrawConfig(input, TOKEN)).rejects.toThrow(`Draw sort with ID ${sortId} does not exist.`);
  });

  describe("Update Draw Config", () => {
    it("Should update a config", async () => {
      const { id: drawConfigId, ...original } = await createTestDrawConfig();

      const input: UpdateDrawConfigInput = {
        drawConfigId,
        name: `Test Draw Config ${ulid()}`,
      };

      await updateDrawConfig(input, TOKEN);

      let config = await getDrawConfigEntity({ drawConfigId, organizationId: ORG_ID });
      expect(config).toMatchObject({
        id: expect.stringMatching(/^draw_config_/),
        name: input.name,
        sortId: original.sortId,
        usePoints: original.usePoints,
        quotaRuleFlags: original.quotaRules.ruleFlags,
      });

      input.quotaRules = {
        ruleFlags: [DrawConfigQuotaRule.FlowQuota],
      };

      await updateDrawConfig(input, TOKEN);

      config = await getDrawConfigEntity({ drawConfigId, organizationId: ORG_ID });
      expect(config).toMatchObject({
        id: expect.stringMatching(/^draw_config_/),
        name: input.name,
        sortId: original.sortId,
        usePoints: original.usePoints,
        quotaRuleFlags: input.quotaRules.ruleFlags,
      });
    });

    it("Should fail to update if sort is not found", async () => {
      const { id: drawConfigId } = await createTestDrawConfig();

      const input: UpdateDrawConfigInput = {
        drawConfigId,
        sortId: `sort_${ulid()}`,
      };

      await expect(updateDrawConfig(input, TOKEN)).rejects.toThrow(`Draw sort with ID ${input.sortId} does not exist.`);
    });

    it("Should throw an error if the ID string is invalid", async () => {
      const input: UpdateDrawConfigInput = {
        drawConfigId: "invalid_id",
        name: `Test Draw Config ${ulid()}`,
      };

      await expect(updateDrawConfig(input, TOKEN)).rejects.toThrowError(
        `Invalid input: {"formErrors":[],"fieldErrors":{"drawConfigId":["Invalid ID"]}}`,
      );
    });
  });

  describe("Delete Draw Config", () => {
    it("Should delete a config", async () => {
      const { id: drawConfigId } = await createTestDrawConfig();

      const configBeforeDelete = await getDrawConfigEntity({ drawConfigId, organizationId: ORG_ID });
      expect(configBeforeDelete).toBeDefined();

      await deleteDrawConfig({ id: drawConfigId }, TOKEN);

      const configAfterDelete = await getDrawConfigEntity({ drawConfigId, organizationId: ORG_ID });
      expect(configAfterDelete).toBeUndefined();
    });

    it("Should delete a config for a workflow node", async () => {
      const workflowNodeId = `workflow_node_${ulid()}`;
      const { id: drawConfigId } = await createTestDrawConfig({ workflowNodeId });

      const configBeforeDelete = await getDrawConfigEntity({ drawConfigId, organizationId: ORG_ID });
      expect(configBeforeDelete).toBeDefined();

      await deleteDrawConfigForWorkflowNode({ workflowNodeId }, TOKEN);

      const configAfterDelete = await getDrawConfigEntity({ drawConfigId, organizationId: ORG_ID });
      expect(configAfterDelete).toBeUndefined();
    });

    it("Should throw an error if the ID string is invalid", async () => {
      const id = "invalid_id";
      await expect(deleteDrawConfig({ id }, TOKEN)).rejects.toThrowError(
        `Invalid input: {"formErrors":["Invalid ID"],"fieldErrors":{}}`,
      );
    });
  });

  describe("Get Draw Config", () => {
    it("Should get a config", async () => {
      const drawConfig = await createTestDrawConfig();
      expect(await getDrawConfig(drawConfig.id, TOKEN)).toMatchObject({
        id: drawConfig.id,
        name: drawConfig.name,
        sortId: drawConfig.sortId,
        usePoints: drawConfig.usePoints,
      });
    });

    it("Should throw if config is not found", async () => {
      const drawConfigId = `draw_config_${ulid()}`;
      await expect(getDrawConfig(drawConfigId, TOKEN)).rejects.toThrow(
        `Draw config with ID ${drawConfigId} does not exist.`,
      );
    });

    it("Should throw an error if the ID is invalid", async () => {
      const id = "invalid_id";
      await expect(getDrawConfig(id, TOKEN)).rejects.toThrowError(
        `Invalid input: {"formErrors":["Invalid ID"],"fieldErrors":{}}`,
      );
    });
  });
});
