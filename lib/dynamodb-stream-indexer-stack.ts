import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as efs from "aws-cdk-lib/aws-efs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as event_sources from "aws-cdk-lib/aws-lambda-event-sources";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as constructs from "constructs";

export class DynamodbStreamIndexerStack extends cdk.Stack {
  constructor(scope: constructs.Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const vpc = new ec2.Vpc(this, "VPC", {
      natGateways: 0,
      maxAzs: 2,
      gatewayEndpoints: {
        dynamodb: {
          service: ec2.GatewayVpcEndpointAwsService.DYNAMODB, // NOTE: So we can talk to Dynamo from the index_reader lambda function
        },
      },
    });

    const table = new dynamodb.Table(this, "Table", {
      partitionKey: {
        name: "PK",
        type: dynamodb.AttributeType.STRING,
      },
      sortKey: {
        name: "SK",
        type: dynamodb.AttributeType.STRING,
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      stream: dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const fileSystem = new efs.FileSystem(this, "Efs", {
      vpc,
      encrypted: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    const accessPoint = fileSystem.addAccessPoint("AccessPoint", {
      path: "/export/lambda",
      createAcl: {
        ownerUid: "1001",
        ownerGid: "1001",
        permissions: "750",
      },
      posixUser: {
        uid: "1001",
        gid: "1001",
      },
    });

    const lambdaFilesystem = lambda.FileSystem.fromEfsAccessPoint(
      accessPoint,
      "/mnt/msg"
    );

    const indexWriterFunction = new lambda.Function(
      this,
      "IndexWriterFunction",
      {
        timeout: cdk.Duration.seconds(60),
        code: lambda.Code.fromAsset("./build/index_writer.zip"),
        handler: "rust-runtime",
        memorySize: 2048,
        runtime: lambda.Runtime.PROVIDED_AL2,
        filesystem: lambdaFilesystem,
        reservedConcurrentExecutions: 1, // NOTE: Tantivy can only have a single index writer, so we need to make this a singleton function
        vpc: vpc,
        environment: {
          EFS_MOUNT_PATH: lambdaFilesystem.config.localMountPath,
          RUST_LOG: "info",
        },
        onFailure: new event_sources.SqsDlq(
          new sqs.Queue(this, "IndexWriterFunctionDLQ", {
            removalPolicy: cdk.RemovalPolicy.DESTROY,
          })
        ),
      }
    );

    indexWriterFunction.addEventSource(
      new event_sources.DynamoEventSource(table, {
        enabled: true,
        startingPosition: lambda.StartingPosition.TRIM_HORIZON,
        batchSize: 1000,
        maxBatchingWindow: cdk.Duration.seconds(60),
        bisectBatchOnError: false,
        retryAttempts: 0,
        parallelizationFactor: 1, // NOTE: Tantivy can only have a single index writer, so we cannot index in parallel, don't worry it's fast
        reportBatchItemFailures: true,
        tumblingWindow: undefined,
        maxRecordAge: undefined,
        onFailure: new event_sources.SqsDlq(
          new sqs.Queue(this, "IndexWriterDynamoStreamDLQ", {
            removalPolicy: cdk.RemovalPolicy.DESTROY,
          })
        ),
      })
    );

    const indexReaderFunction = new lambda.Function(
      this,
      "IndexReaderFunction",
      {
        timeout: cdk.Duration.seconds(30),
        code: lambda.Code.fromAsset("./build/index_reader.zip"),
        handler: "rust-runtime",
        memorySize: 512,
        runtime: lambda.Runtime.PROVIDED_AL2,
        filesystem: lambdaFilesystem,
        vpc: vpc,
        environment: {
          EFS_MOUNT_PATH: lambdaFilesystem.config.localMountPath,
          RUST_LOG: "info",
          TABLE_NAME: table.tableName,
        },
      }
    );

    table.grantReadData(indexReaderFunction);

    new cdk.CfnOutput(this, "TableName", {
      value: table.tableName,
    });
  }
}
