import 'source-map-support/register';
import { ConfigParser } from '@projectkittyhawk/core-config';
import { App, Aspects } from 'aws-cdk-lib';
import * as cdk from 'aws-cdk-lib';
import { AwsSolutionsChecks } from 'cdk-nag';
import { ServiceConfigLoader } from '../lib/common/service-config-loader';
import { DpsDatamartOrchestrationStack } from '../lib/dps-datamart-orchestration-stack';
import { IHEServiceCommonOrchestrationStack } from '../lib/dps-ihe-service-orchestration-common';
import { IHEOrchestrationStack } from '../lib/dps-ihe-service-orchestration-stack';
import { GlueResourcesStack } from '../lib/glue-resources';
import { ReferenceDataPipeline } from '../lib/reference-data-pipeline';
import { ReferenceDataService } from '../lib/reference-data-service';
import { Service } from '../lib/types/service';

// global parameters
const app = new App();
const config = new ConfigParser();
const envName = config.get('envName');
const region = config.get('region');
const ds_account = config.get('data-services-account');
const is_account = config.get('integration-services-account');
const crawlerBucketPath = 'referencedata';

const vpcEndpointId: string = config.get('vpcEndpointId');
const apiSchemaLocation: string = config.get('apiSchemaLocation');

const serviceName = 'dps-sp-referencedata';
const service = 'referencedata';
const transformServiceName = 'dps-ihe-transformation';

// console.log('🌍 Environment:', envName);
// console.log('📍 Region:', region);
// console.log('📦 DS Account:', ds_account);
// console.log('📦 IS Account:', is_account);

// stack name
const ref_data_pipeline = new ReferenceDataPipeline(
  app,
  `${envName}-reference-data-pipeline`,
  {
    env: { account: ds_account, region: region },
    envName,
    serviceName,
    service,
    crawlerBucketPath,
  }
);

const ref_data_service = new ReferenceDataService(
  app,
  `${envName}-reference-data-service`,
  {
    env: { account: ds_account, region },
    envName,
    serviceName,
    service,
    apiSchemaLocation,
    vpcEndpointId,
  }
);
ref_data_service.addDependency(ref_data_pipeline);

const iheServiceCommonOrchestration = new IHEServiceCommonOrchestrationStack(
  app,
  `${envName}-ihe-common-service-orchestration`,
  {
    env: { account: ds_account, region: region },
    envName,
  }
);

new GlueResourcesStack(
  app,
  `${envName}-${transformServiceName}-gluejobs`,
  is_account,
  {
    env: { account: ds_account, region: region },
    envName: envName,
    serviceName: transformServiceName,
  }
);

const serviceConfigLoader = new ServiceConfigLoader();
const services =
  serviceConfigLoader.loadServiceConfigsFileContentforEnvironmnet(envName);

if (services) {
  services.forEach((service: Service) => {
    const systemName =
      `${service.tenantCode.toLowerCase()}-${service.system.toLowerCase()}`.replaceAll(
        '_',
        '-'
      );

    const iheServiceOrchestration = new IHEOrchestrationStack(
      app,
      `${envName}-${systemName}-service-orchestration`,
      {
        env: { account: ds_account, region: region },
        envName,
        service: service,
      }
    );
    iheServiceOrchestration.addDependency(iheServiceCommonOrchestration);
  });
}

// DATAMART STACK INSTANTIATION (MOVED AND HARDCODED/CONFIGURABLE)
const datamartStackId = `${envName}-datamart-curated-db-service-orchestration`;

// Configuration values for the DpsDatamartOrchestrationStack
const datamartTenantCode = 'DATAMART';
const datamartSystem = 'CURATED_DB_SERVICE';
const internalSystemNameForDatamart =
  `${datamartTenantCode.toLowerCase()}-${datamartSystem.toLowerCase()}`.replace(
    '_',
    '-'
  );

const datamartCuratedDbNameValue = 'datamart_curated_db';
const datamartEtlScheduleValue = 'cron(30 10 * * ? *)';
const datamartCrawlerScheduleValue = 'cron(15 10 * * ? *)';

new DpsDatamartOrchestrationStack(app, datamartStackId, {
  env: { account: ds_account, region: region },
  envName: envName,
  internalSystemName: internalSystemNameForDatamart,
  curatedDatabaseName: datamartCuratedDbNameValue,
  etlJobSchedule: datamartEtlScheduleValue,
  crawlerJobSchedule: datamartCrawlerScheduleValue,
  isServiceEnabled: true,
  crawlerEntities: ['domain=admissions'],
  integrationServicesAccountNumber: is_account,
});

// Add tags
cdk.Tags.of(app).add('service', transformServiceName);
cdk.Tags.of(app).add('repo', 'dps-data-services-ihe');
cdk.Tags.of(app).add('costcenter', '7315');
cdk.Tags.of(app).add('environment', envName);
cdk.Tags.of(app).add('domain', 'admissions');

// CDK-Nag checks
Aspects.of(app).add(new AwsSolutionsChecks({ verbose: true }));
