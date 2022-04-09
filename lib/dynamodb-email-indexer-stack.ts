import * as cdk from "aws-cdk-lib";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as efs from "aws-cdk-lib/aws-efs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as event_sources from "aws-cdk-lib/aws-lambda-event-sources";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as constructs from "constructs";

export class DynamodbEmailIndexerStack extends cdk.Stack {
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

    const emailTable = new dynamodb.Table(this, "EmailTable", {
      partitionKey: {
        name: "id",
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

    const emailIndexWriterFunction = new lambda.Function(
      this,
      "EmailIndexWriterFunction",
      {
        timeout: cdk.Duration.seconds(60),
        code: lambda.Code.fromAsset("./build/email_index_writer.zip"),
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
          new sqs.Queue(this, "EmailIndexWriterFunctionDLQ", {
            removalPolicy: cdk.RemovalPolicy.DESTROY,
          })
        ),
      }
    );

    emailIndexWriterFunction.addEventSource(
      new event_sources.DynamoEventSource(emailTable, {
        enabled: true,
        startingPosition: lambda.StartingPosition.TRIM_HORIZON,
        batchSize: 1000,
        maxBatchingWindow: cdk.Duration.seconds(30),
        bisectBatchOnError: false,
        retryAttempts: 0,
        parallelizationFactor: 1, // NOTE: Tantivy can only have a single index writer, so we cannot index in parallel
        reportBatchItemFailures: true,
        tumblingWindow: undefined,
        maxRecordAge: undefined,
        onFailure: new event_sources.SqsDlq(
          new sqs.Queue(this, "EmailIndexWriterDynamoStreamDLQ", {
            removalPolicy: cdk.RemovalPolicy.DESTROY,
          })
        ),
      })
    );

    const emailIndexReaderFunction = new lambda.Function(
      this,
      "EmailIndexReaderFunction",
      {
        timeout: cdk.Duration.seconds(30),
        code: lambda.Code.fromAsset("./build/email_index_reader.zip"),
        handler: "rust-runtime",
        memorySize: 2048,
        runtime: lambda.Runtime.PROVIDED_AL2,
        filesystem: lambdaFilesystem,
        vpc: vpc,
        environment: {
          EFS_MOUNT_PATH: lambdaFilesystem.config.localMountPath,
          RUST_LOG: "info",
          TABLE_NAME: emailTable.tableName,
        },
      }
    );

    emailTable.grantReadData(emailIndexReaderFunction);

    // TODO: Using escape hatch until CDK support for function urls
    // https://github.com/aws/aws-cdk/pull/19817
    // https://github.com/aws/aws-cdk/issues/19798
    const emailIndexReaderUrl = new cdk.CfnResource(
      this,
      "EmailIndexReaderUrl",
      {
        type: "AWS::Lambda::Url",
        properties: {
          AuthType: "AWS_IAM",
          TargetFunctionArn: emailIndexReaderFunction.functionArn,
        },
      }
    );

    new cdk.CfnOutput(this, "EmailTableName", {
      value: emailTable.tableName,
    });

    new cdk.CfnOutput(this, "EmailIndexReaderFunctionUrl", {
      value: emailIndexReaderUrl.getAtt("FunctionUrl").toString(),
    });
  }
}
