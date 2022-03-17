#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import * as dotenv from "dotenv";
import { get } from "env-var";
import { DynamodbStreamIndexerStack } from "../lib/dynamodb-stream-indexer-stack";

dotenv.config();

const CDK_DEFAULT_ACCOUNT = get("CDK_DEFAULT_ACCOUNT").required().asString();
const CDK_DEFAULT_REGION = get("CDK_DEFAULT_REGION").required().asString();

const app = new cdk.App();
const stack = new DynamodbStreamIndexerStack(app, "dynamodb-stream-indexer", {
  env: {
    account: CDK_DEFAULT_ACCOUNT,
    region: CDK_DEFAULT_REGION,
  },
});
