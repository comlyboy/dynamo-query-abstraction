import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, ScanCommand, QueryCommand, DeleteCommand, PutCommand } from "@aws-sdk/lib-dynamodb"
import { NodeHttpHandler } from "@aws-sdk/node-http-handler";
import { logger } from '../../lib/serverLogger';
import * as Sqs from "../../models/Sqs";
import constants from '../../lib/constants';
const { DYNAMODB_ENDPOINT } = constants;
const nodeEnv = process.env.NODE_ENV;
const testMode = process.env.TEST_MODE;
const isLocalDockerMode = testMode === 'integration' || testMode === 'cors' || nodeEnv === 'local';

let dynamoClient;
let docClient;

export function initDynamoDBClient(region, params) {
	const marshallOptions = {};
	const dynamoDbConfig = {
		region,
		requestHandler: new NodeHttpHandler({
			connectionTimeout: 29000,
			socketTimeout: 30000
		})
	}
	if (isLocalDockerMode) {
		logger.info('In local docker mode...');
		dynamoDbConfig.endpoint = params[DYNAMODB_ENDPOINT];
		dynamoDbConfig.credentials = {
			accessKeyId: 'test',
			secretAccessKey: 'test',
		};
		marshallOptions.removeUndefinedValues = true;
	}
	dynamoClient = new DynamoDBClient(dynamoDbConfig);
	docClient = DynamoDBDocumentClient.from(dynamoClient, { marshallOptions });
	logger.info('DynamoDBClient initialized');
}

/**
 * Calls DB with the operation and the parameters
 *
 * @param {*} method
 * @param {*} params
 */

// add third param to get SQS flag and add logic to push to sqs when operation fails and flag is true
export function dbOps(method, params, sqsOptions, pushToQueue = false) {
	// eslint-disable-next-line no-async-promise-executor
	return new Promise(async (resolve, reject) => {
		logger.info("Called DB with method: '" + method + "' and params :" + JSON.stringify(params))
		const { tableName } = sqsOptions;
		params = {
			...params,
			TableName: tableName,
		}
		let command = buildCommand(method, params);
		try {
			const res = await docClient.send(command);
			if (res && res.LastEvaluatedKey) {
				let pageResult = { LastEvaluatedKey: res.LastEvaluatedKey };
				while (pageResult.LastEvaluatedKey) {
					params.ExclusiveStartKey = pageResult.LastEvaluatedKey;
					command = buildCommand(method, params);
					pageResult = await docClient.send(command);
					res.Items.push(...pageResult.Items);
					res.Count += pageResult.Count;
					res.ScannedCount += pageResult.ScannedCount;
				}
				delete res.LastEvaluatedKey;
			}
			logger.info(" Response from DB successful ")
			resolve(res);
		} catch (err) {
			if (pushToQueue && !sqsOptions.sqsDisabled) {
				//push to queue
				Sqs.sendMessage({
					method,
					operator: 'DB',
					payload: params
				}, sqsOptions);
				logger.error("ERROR FROM DB" + JSON.stringify(err) + " pushing to queue")
				reject(err);
			} else {
				logger.error("ERROR FROM DB:: " + err)
				reject(err);
			}
		}
	});
}

const buildCommand = (method, params) => {
	let map = {
		'query': QueryCommand,
		'scan': ScanCommand,
		'put': PutCommand,
		'delete': DeleteCommand
	}
	return new map[method](params);
}

