import { RemovalPolicy, Stack, StackProps, Tags } from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import { NagSuppressions } from 'cdk-nag';
import { Construct } from 'constructs';

export interface CuratedBucketsProps extends StackProps {
  readonly envName: string;
  readonly dataServiceAccount?: string;
  readonly kmsKey: kms.IKey;
  readonly accessLogBucket: s3.IBucket;
  readonly bucketNames: string[];
}

export class CuratedBuckets extends Construct {
  private readonly stack = Stack.of(this);
  private readonly stackName = this.stack.stackName;
  private readonly account = this.stack.account;

  constructor(scope: Construct, id: string, props: CuratedBucketsProps) {
    super(scope, id);

    const {
      envName,
      dataServiceAccount,
      kmsKey,
      accessLogBucket,
      bucketNames,
    } = props;

    if (dataServiceAccount === undefined) {
      throw new Error(
        'dataServiceAccount parameter is required for CuratedBuckets construct'
      );
    }

    bucketNames.forEach((bucketName) => {
      const curatedBucket = new s3.Bucket(
        this,
        `${this.stackName}-${bucketName}`,
        {
          bucketName: `${envName}-${bucketName}-${this.account}`,
          removalPolicy: RemovalPolicy.DESTROY,
          encryption: s3.BucketEncryption.KMS,
          encryptionKey: kmsKey,
          versioned: true,
          blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
          enforceSSL: true,
          eventBridgeEnabled: true,
          bucketKeyEnabled: true,
          serverAccessLogsBucket: accessLogBucket,
        }
      );
      Tags.of(curatedBucket).add('DataClassification', 'Private');

      curatedBucket.addToResourcePolicy(
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            's3:ListBucket',
            's3:GetObject',
            's3:PutObject',
            's3:GetBucketAcl',
            's3:PutObjectAcl',
          ],
          resources: [
            `arn:aws:s3:::${curatedBucket.bucketName}`,
            `arn:aws:s3:::${curatedBucket.bucketName}/*`,
          ],
          principals: [new iam.ServicePrincipal('appflow.amazonaws.com')],
          conditions: {
            StringEquals: { 'aws:SourceAccount': `${this.account}` },
          },
        })
      );

      curatedBucket.addToResourcePolicy(
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            's3:ListBucket',
            's3:GetObject',
            's3:PutObject',
            's3:DeleteObject',
          ],
          resources: [
            `arn:aws:s3:::${curatedBucket.bucketName}`,
            `arn:aws:s3:::${curatedBucket.bucketName}/*`,
          ],
          principals: [new iam.ServicePrincipal('lambda.amazonaws.com')],
          conditions: {
            StringEquals: { 'aws:SourceAccount': `${this.account}` },
          },
        })
      );

      curatedBucket.grantReadWrite(
        new iam.ArnPrincipal(
          `arn:aws:iam::${dataServiceAccount}:role/${envName}-dps-iam-glue-orchestration-role`
        )
      );
      curatedBucket.grantPutAcl(
        new iam.ArnPrincipal(
          `arn:aws:iam::${dataServiceAccount}:role/${envName}-dps-iam-glue-orchestration-role`
        )
      );

      new ssm.StringParameter(this, `${this.stackName}-${bucketName}Arn`, {
        description: `${bucketName} Bucket ARN`,
        parameterName: `/cdk/${envName}/dps-data-services-baseline/${bucketName}-bucket-arn`,
        stringValue: curatedBucket.bucketArn,
      });

      new ssm.StringParameter(
        this,
        `${this.stackName}-${bucketName}CrossAccountArn`,
        {
          description: `${bucketName} Bucket ARN`,
          parameterName: `/services/${envName}/${bucketName}-bucket-arn`,
          stringValue: curatedBucket.bucketArn,
        }
      );
    });

    NagSuppressions.addStackSuppressions(this.stack, [
      { id: 'AwsSolutions-IAM4', reason: 'Uses AWS managed policies' },
    ]);

    NagSuppressions.addStackSuppressions(this.stack, [
      { id: 'AwsSolutions-IAM5', reason: 'Uses AWS managed policies' },
    ]);
  }
}
