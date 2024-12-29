import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as rds from "aws-cdk-lib/aws-rds";

export class RdsDataStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const vpc = new ec2.Vpc(this, "Vpc", {
      ipAddresses: ec2.IpAddresses.cidr("10.0.0.0/16"),
      natGateways: 0,
    });

    const mysqlCluster = new rds.DatabaseCluster(this, "MySqlCluster", {
      engine: rds.DatabaseClusterEngine.auroraMysql({
        version: rds.AuroraMysqlEngineVersion.VER_3_08_0,
      }),
      writer: rds.ClusterInstance.serverlessV2("writer"),
      serverlessV2MaxCapacity: 2,
      serverlessV2MinCapacity: 0,
      enableDataApi: true,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
      },
      vpc,
    });

    const postgresCluster = new rds.DatabaseCluster(this, "PostgresCluster", {
      engine: rds.DatabaseClusterEngine.auroraPostgres({
        version: rds.AuroraPostgresEngineVersion.VER_16_6,
      }),
      writer: rds.ClusterInstance.serverlessV2("writer"),
      serverlessV2MaxCapacity: 2,
      serverlessV2MinCapacity: 0,
      enableDataApi: true,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PRIVATE_ISOLATED,
      },
      vpc,
    });
  }
}
