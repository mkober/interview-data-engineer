/* eslint-disable @typescript-eslint/no-explicit-any */
import * as fs from 'fs';
import * as path from 'path';
import { Stack, StackProps } from 'aws-cdk-lib';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import { IRole } from 'aws-cdk-lib/aws-iam';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { IBucket } from 'aws-cdk-lib/aws-s3';
import * as s3Deploy from 'aws-cdk-lib/aws-s3-deployment';
import * as ssm from 'aws-cdk-lib/aws-ssm';
import { NagSuppressions } from 'cdk-nag';
import { Construct } from 'constructs';
import * as commonLib from './common/common';

interface GlueResourceProps extends StackProps {
  readonly envName: string;
  readonly serviceName: string;
}

class JobConfigLoader {
  private getFilesRecursively(dir: string, fileList: string[] = []): string[] {
    const files = fs.readdirSync(dir);
    for (const file of files) {
      const filePath = path.join(dir, file); // nosemgrep: javascript.lang.security.audit.path-traversal.path-join-resolve-traversal.path-join-resolve-traversal
      const stats = fs.statSync(filePath);
      if (stats.isDirectory()) {
        this.getFilesRecursively(filePath, fileList);
      } else if (file.endsWith('.json')) {
        fileList.push(filePath);
      }
    }
    return fileList;
  }

  public loadJobConfigsFiles(): string[] {
    const baseDir = './config/glue-job-configs';
    const fileNames = this.getFilesRecursively(baseDir);
    return fileNames;
  }

  public loadJobConfigsFileContent(
    fileNames: string[],
    envName: string,
    dsAccountNumber: string,
    isAccountNumber: string,
    serviceName: string
  ): any[] {
    const jsonJobConfigs = [];
    for (const filePath of fileNames) {
      const fileContent = fs.readFileSync(filePath, 'utf8');
      const data = JSON.parse(
        fileContent
          .replace(/\{ds_account_number\}/g, dsAccountNumber)
          .replace(/\{is_account_number\}/g, isAccountNumber)
          .replace(/\{env_name\}/g, envName)
          .replace(/\{service_name\}/g, serviceName)
      );
      jsonJobConfigs.push(data);
    }

    return jsonJobConfigs;
  }
}

export class GlueResourcesStack extends Stack {
  glueRoleArn: string;
  glueRole: IRole;
  sfnRoleArn: string;
  sfnRole: IRole;
  dataArtifactsBucketArn: string;
  s3DataArtifactBucket: IBucket;
  dataArtifactBucket: commonLib.GetBucketFromArn;
  glueConnectionNames: string[];
  glueSecurityConf: glue.CfnSecurityConfiguration;

  constructor(
    scope: Construct,
    id: string,
    is_account_number: string,
    props: GlueResourceProps
  ) {
    super(scope, id, props);

    const { envName, serviceName } = props;
    // Initialize all properties from SSM parameter needed to create this stack
    this.initializeStackProperties(envName);
    //Job Configuration files loader
    const jobConfigLoader = new JobConfigLoader();

    const fileNames = jobConfigLoader.loadJobConfigsFiles();
    const configData = jobConfigLoader.loadJobConfigsFileContent(
      fileNames,
      envName,
      this.account,
      is_account_number,
      props.serviceName
    );

    // console.log(
    //   '📦 Loaded Glue Job Configs:',
    //   configData.map((j) => j.jobConfig.name)
    // );

    for (let i = 0; i < configData.length; i++) {
      const configDataItem = configData[i];
      const glueAdmissionsTransformationJobPath = `s3://${this.s3DataArtifactBucket.bucketName}/assets/setup/${serviceName}/gluejobs/${configDataItem['jobConfig'].scriptLocationPrefix}/${configDataItem['jobConfig'].scriptName}`;
      new s3Deploy.BucketDeployment(
        this,
        `${configDataItem['jobConfig'].name}-s3deploy`,
        {
          sources: [
            s3Deploy.Source.asset(
              configDataItem['jobConfig'].assetFolderLocation
            ),
          ],
          destinationBucket: this.dataArtifactBucket.BucketName,
          destinationKeyPrefix: `assets/setup/${serviceName}/gluejobs/${configDataItem['jobConfig'].scriptLocationPrefix}/`,
        }
      );
      const glueTransformationJob = new glue.CfnJob(
        this,
        `${configDataItem['jobConfig'].name}`,
        {
          defaultArguments: configDataItem['jobConfig'].jobParameters,
          name: `${configDataItem['jobConfig'].name}`,
          description: 'Glue job for pkh admissions transformation',
          role: this.glueRole.roleArn,
          connections: {
            connections: this.glueConnectionNames,
          },
          securityConfiguration: this.glueSecurityConf.ref,
          executionProperty: {
            maxConcurrentRuns: 15,
          },
          command: {
            name: 'glueetl',
            pythonVersion: '3',
            scriptLocation: glueAdmissionsTransformationJobPath,
          },
          timeout: 60,
          numberOfWorkers: 5,
          glueVersion: '5.0',
          workerType: 'G.1X',
        }
      );
      const schedulerParamVal = configDataItem.jobConfig.schedule[envName];
      if (schedulerParamVal != null && schedulerParamVal != '') {
        const glueTransformationJobSchedule = new glue.CfnTrigger(
          this,
          `${configDataItem['jobConfig'].name}-schedule`,
          {
            name: `${configDataItem['jobConfig'].name}-schedule`,
            type: 'SCHEDULED',
            actions: [
              {
                jobName: glueTransformationJob.name,
              },
            ],
            schedule: schedulerParamVal,
            startOnCreation: true,
          }
        );
        glueTransformationJobSchedule.node.addDependency(glueTransformationJob);
      }
    }

    NagSuppressions.addResourceSuppressionsByPath(
      this,
      `/${id}/Custom::CDKBucketDeployment8693BB64968944B69AAFB0CC9EB8756C/Resource`,
      [
        {
          id: 'AwsSolutions-L1',
          reason:
            'CDK bucket deployment Lambda does not need to use latest runtime',
        },
      ]
    );

    NagSuppressions.addResourceSuppressionsByPath(
      this,
      `/${id}/Custom::CDKBucketDeployment8693BB64968944B69AAFB0CC9EB8756C/ServiceRole/Resource`,
      [
        {
          id: 'AwsSolutions-IAM4',
          reason: 'The IAM user, role, or group uses AWS managed policies',
        },
      ]
    );

    NagSuppressions.addResourceSuppressionsByPath(
      this,
      `/${id}/Custom::CDKBucketDeployment8693BB64968944B69AAFB0CC9EB8756C/ServiceRole/DefaultPolicy/Resource`,
      [
        {
          id: 'AwsSolutions-IAM4',
          reason: 'The IAM user, role, or group uses AWS managed policies',
        },
        {
          id: 'AwsSolutions-IAM5',
          reason: 'The IAM user, role, or group uses AWS managed policies',
        },
      ]
    );

    NagSuppressions.addResourceSuppressions(
      this.glueRole,
      [
        {
          id: 'AwsSolutions-IAM5',
          reason: 'The IAM user, role, or group uses AWS managed policies',
        },
      ],
      true
    );

    //////////////////////////////////////////////////////////////////
  }

  private initializeStackProperties(envName: string) {
    this.glueRoleArn = ssm.StringParameter.fromStringParameterName(
      this,
      `${this.stackName}-glueRoleArn`,
      `/cdk/${envName}/dps-data-services-baseline/glue-role-arn`
    ).stringValue;

    this.glueRole = iam.Role.fromRoleArn(
      this,
      `${this.stackName}-glueRole`,
      this.glueRoleArn
    );

    this.sfnRoleArn = ssm.StringParameter.fromStringParameterName(
      this,
      `${this.stackName}-sfnRoleArn`,
      `/cdk/${envName}/dps-data-services-baseline/sfn-role-arn`
    ).stringValue;

    this.sfnRole = iam.Role.fromRoleArn(
      this,
      `${this.stackName}-sfnRole`,
      this.sfnRoleArn
    );

    this.dataArtifactsBucketArn = ssm.StringParameter.fromStringParameterName(
      this,
      `${this.stackName}-dataArtifactsBucketArn`,
      `/cdk/${envName}/dps-data-services-baseline/data-artifact-bucket-arn`
    ).stringValue;

    this.s3DataArtifactBucket = s3.Bucket.fromBucketArn(
      this,
      `${this.stackName}-dataArtifactsBucket`,
      this.dataArtifactsBucketArn
    );

    this.dataArtifactBucket = new commonLib.GetBucketFromArn(
      this,
      `${this.stackName}-iheOrchestrationArtifactBucket`,
      {
        stackName: this.stackName,
        path: `/cdk/${envName}/dps-data-services-baseline/data-artifact-bucket-arn`,
        envName: envName,
      }
    );

    ///cdk/${envName}/dps-data-services-baseline/glue-connection-list
    this.glueConnectionNames =
      ssm.StringListParameter.valueForTypedListParameter(
        this,
        `/cdk/${envName}/dps-data-services-baseline/glue-connection-list`
      );

    const glueKMSKeyArn = ssm.StringParameter.fromStringParameterAttributes(
      this,
      `${this.stackName}-glueKMSKeyArn`,
      {
        parameterName: `/cdk/${envName}/dps-data-services-baseline/glue-kms-key-arn`,
      }
    ).stringValue;
    const glueKMSKey = kms.Key.fromKeyArn(
      this,
      `${this.stackName}-glueKMSKey`,
      glueKMSKeyArn
    );

    this.glueSecurityConf = new glue.CfnSecurityConfiguration(
      this,
      `${this.stackName}-GlueIHETransformationSecurityConfiguration`,
      {
        encryptionConfiguration: {
          s3Encryptions: [
            {
              kmsKeyArn: glueKMSKey.keyArn,
              s3EncryptionMode: 'SSE-KMS',
            },
          ],
          cloudWatchEncryption: {
            cloudWatchEncryptionMode: 'SSE-KMS',
            kmsKeyArn: glueKMSKey.keyArn,
          },
          jobBookmarksEncryption: {
            jobBookmarksEncryptionMode: 'CSE-KMS',
            kmsKeyArn: glueKMSKey.keyArn,
          },
        },
        name: `${this.stackName}-glueKMSKey`,
      }
    );
  }
}
