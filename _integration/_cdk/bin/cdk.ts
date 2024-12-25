#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { RdsDataStack } from "../lib/rds-data-stack";

const app = new cdk.App();
new RdsDataStack(app, "RdsDataStack", {});
