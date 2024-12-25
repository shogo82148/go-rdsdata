import * as cdk from "aws-cdk-lib";
import { Template } from "aws-cdk-lib/assertions";
import * as Cdk from "../lib/rds-data-stack";

test("Snapshot Test", () => {
  const app = new cdk.App();
  const stack = new Cdk.RdsDataStack(app, "MyTestStack");
  const template = Template.fromStack(stack);
  expect(template.toJSON()).toMatchSnapshot();
});
