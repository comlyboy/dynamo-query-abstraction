import constants from '../../lib/constants';

const {
	TABLE_NAME,
	META_DB_FIELDS,
	TABLE_DELIMITER,
	ATTRIBUTE_MODEL_FIELDS,
	USER_ATTRIBUTE_MODEL_FIELDS,
	INDEXES: {
		APPALIAS_NAME,
		APPALIAS_GROUP,
		APPALIAS_TENANT,
		APPALIAS_TOKEN
	} } = constants

function TableParams(tenantId) {
	if (tenantId == "META") {
		return {
			...META_DB_FIELDS,
			TableName: TABLE_NAME
		}
	} else {
		return {
			...ATTRIBUTE_MODEL_FIELDS,
			TableName: TABLE_NAME
		}
	}
}

function TableParamsForUserAttrs() {
	return {
		...USER_ATTRIBUTE_MODEL_FIELDS,
		TableName: TABLE_NAME
	}
}

export function getMetaData(appAlias, VERSION) {
	return {
		TableName: TABLE_NAME,
		KeyConditionExpression: "tenantId = :t and appAlias_name = :ag",
		ExpressionAttributeValues: {
			":t": 'META',
			":ag": `${appAlias}${TABLE_DELIMITER}${VERSION}`
		}
	}
}

export function getAllAttributes(appAlias, version, tenantId = "META", isFromUI = false) {
	if (isFromUI) {
		return {
			...TableParams(tenantId),
			IndexName: APPALIAS_TENANT,
			KeyConditionExpression: "tenantId = :t and appAlias_version = :a",
			FilterExpression: "isPrivate = :p",
			ExpressionAttributeValues: {
				":t": tenantId,
				":a": `${appAlias}${TABLE_DELIMITER}${version}`,
				":p": false
			}
		}
	} else {
		return {
			...TableParams(tenantId),
			IndexName: APPALIAS_TENANT,
			KeyConditionExpression: "tenantId = :t and appAlias_version = :a",
			ExpressionAttributeValues: {
				":t": tenantId,
				":a": `${appAlias}${TABLE_DELIMITER}${version}`
			}
		}
	}
}

export function getAllTokenAttributes(appAlias,  tenantId = "META",version, inToken = true) {
	return {
		...TableParams(tenantId),
		IndexName: APPALIAS_TOKEN,
		KeyConditionExpression: "tenantId = :t and appAlias_inToken = :at",
		ExpressionAttributeValues: {
			":t": tenantId,
			":at": `${appAlias}${TABLE_DELIMITER}${inToken}${TABLE_DELIMITER}${version}`
		}
	}
}

export function getAllAttributesAcrossAnApp(appAlias, name, version) {
	return {
		TableName: TABLE_NAME,
		IndexName: APPALIAS_NAME,
		KeyConditionExpression: 'appAlias_name = :an',
		ExpressionAttributeValues: {
			":an": `${appAlias}${TABLE_DELIMITER}${name}${TABLE_DELIMITER}${version}`
		}
	}
}


export function getAttributesByName(appAlias, version, name, tenantId = "META", isFromUI = false) {
	if (isFromUI) {
		return {
			...TableParams(tenantId),
			KeyConditionExpression: `tenantId = :t and appAlias_name = :an`,
			FilterExpression: "isPrivate = :p",
			ExpressionAttributeValues: {
				":t": tenantId,
				":an": `${appAlias}${TABLE_DELIMITER}${name}${TABLE_DELIMITER}${version}`,
				":p": false
			}
		}
	} else {
		return {
			...TableParams(tenantId),
			KeyConditionExpression: `tenantId = :t and appAlias_name = :an`,
			ExpressionAttributeValues: {
				":t": tenantId,
				":an": `${appAlias}${TABLE_DELIMITER}${name}${TABLE_DELIMITER}${version}`
			}
		}
	}
}

export function getAttributesByGroup(appAlias, version, group, tenantId = "META", isFromUI = false) {
	if (isFromUI) {
		return {
			...TableParams(tenantId),
			IndexName: APPALIAS_GROUP,
			KeyConditionExpression: "tenantId = :t and appAlias_group = :ag",
			FilterExpression: "isPrivate = :p",
			ExpressionAttributeValues: {
				":t": tenantId,
				":ag": `${appAlias}${TABLE_DELIMITER}${group}${TABLE_DELIMITER}${version}`,
				":p": false
			}
		}
	} else {
		return {
			...TableParams(tenantId),
			IndexName: APPALIAS_GROUP,
			KeyConditionExpression: "tenantId = :t and appAlias_group = :ag",
			ExpressionAttributeValues: {
				":t": tenantId,
				":ag": `${appAlias}${TABLE_DELIMITER}${group}${TABLE_DELIMITER}${version}`
			}
		}
	}
}

export function getAllUserAttributes(appAlias, version, tenantId) {
	return {
		...TableParamsForUserAttrs(),
		IndexName: APPALIAS_TENANT,
		KeyConditionExpression: `tenantId = :t AND appAlias_version = :a`,
		FilterExpression: "isPrivate = :p",
		ExpressionAttributeValues: {
			":t": tenantId,
			":a": `${appAlias}${TABLE_DELIMITER}${version}`,
			":p": false
		}
	}
}

export function getUserAttributesByIsUserManaged(appAlias, version, isUserManaged, tenantId) {
	const ium = isUserManaged.toLowerCase() === 'true';
	return {
		...TableParamsForUserAttrs(),
		IndexName: APPALIAS_TENANT,
		KeyConditionExpression: `tenantId = :t AND appAlias_version = :a`,
		FilterExpression: ium ? "isPrivate = :p AND isUserManaged = :um" : "isPrivate = :p AND (isUserManaged = :um OR attribute_not_exists(isUserManaged))",
		ExpressionAttributeValues: {
			":t": tenantId,
			":a": `${appAlias}${TABLE_DELIMITER}${version}`,
			":um": ium,
			":p": false
		}
	}
}

export function getTenantsForAttribute(appAlias, name, version) {
	return {
		TableName: TABLE_NAME,
		IndexName: APPALIAS_NAME,
		KeyConditionExpression: "appAlias_name = :an",
		ExpressionAttributeValues: {
			":an": version ? `${appAlias}${TABLE_DELIMITER}${name}${TABLE_DELIMITER}${version}` : `${appAlias}${TABLE_DELIMITER}${name}`
		}
	}
}

export function fieldObj(field) {
	const obj = {
		"defaultValue": field.defaultValue,
		"description": field.description ? field.description : '',
		"group": field.group,
		"inToken": field.inToken ? field.inToken : false,
		"isPrivate": field.isPrivate ? field.isPrivate : false,
		"name": field.name,
		"type": field.type,
		"validation": field.validation
	};
	// only take up space in dynamodb for set values
	if (field.displayName) {
		obj.displayName = field.displayName;
	}
	if (field.uiProperties) {
		obj.uiProperties = field.uiProperties;
	}
	return obj;
}

export function userFieldObj(field) {
	const obj = {
		"defaultValue": field.defaultValue,
		"description": field.description ? field.description : '',
		"group": field.group,
		"inToken": field.inToken ? field.inToken : false,
		"isPrivate": field.isPrivate ? field.isPrivate : false,
		"isUserManaged": field.isUserManaged ? field.isUserManaged : false,
		"name": field.name,
		"type": field.type,
		"validation": field.validation
	};
	// only take up space in dynamodb for set values
	if (field.displayName) {
		obj.displayName = field.displayName;
	}
	if (field.uiProperties) {
		obj.uiProperties = field.uiProperties;
	}
	return obj;
}

export function updateMetaPayload(payload, appAlias, VERSION) {
	const definitions = {};
	const userDefinitions = {};
	const tenantConf = payload.tenant_conf || [];
	const userConf = payload.user_conf || [];
	tenantConf.map(field => {
		definitions[field.name] = fieldObj(field);
	});
	userConf.map(field => {
		userDefinitions[field.name] = userFieldObj(field);
	});
	return {
		TableName: TABLE_NAME,
		Item: {
			"tenantId": "META",
			"appAlias": appAlias,
			"appAlias_name": `${appAlias}${TABLE_DELIMITER}${VERSION}`,
			definitions,
			userDefinitions
		}
	}
}

export function deleteAttrPayload(payload, appAlias, VERSION) {
	return {
		TableName: TABLE_NAME,
		Key: {
			"tenantId": payload.tenantId,
			"appAlias_name": VERSION ? `${appAlias}${TABLE_DELIMITER}${payload.name}${TABLE_DELIMITER}${VERSION}` : `${appAlias}${TABLE_DELIMITER}${payload.name}`
		}
	}
}

export function getTenantById(tenantId) {
	return {
		TableName: TABLE_NAME,
		"FilterExpression": "tenantId = :tAl",
		"ExpressionAttributeValues": { ":tAl": tenantId }
	}
}

export function getAllUsersPerTenant(tenantId) {
	return {
		TableName: TABLE_NAME,
		IndexName: 'tenantUserId-index',
		KeyConditionExpression: "tenantUserId = :an",
		ExpressionAttributeValues: {
			":an": `${tenantId}${TABLE_DELIMITER}`
		}
	}
}

export function searchUserAttributesByTenantAndAppAlias(tenantId, appAlias, userAttributeSearchBody, userMetaMap, VERSION) {
	const appAliasVersion = `${appAlias}${TABLE_DELIMITER}${VERSION}`;
	const expressionAttributeValues = {
		':a': appAliasVersion,
		':t': tenantId
	};
	const expressionAttributeNames = {
		'#value': 'value'
	};
	let nameIndex = 0,
		valueIndex = 0;
	const filterExpressionParts = [];
	for (const userAttributeSearchQuery of userAttributeSearchBody) {
		const name = userAttributeSearchQuery.name;
		const values = userAttributeSearchQuery.values;
		const nameKey = `:name${nameIndex}`;
		const appAliasName = `${appAlias}${TABLE_DELIMITER}${name}${TABLE_DELIMITER}${VERSION}`;
		const type = userMetaMap[name].type;
		expressionAttributeValues[nameKey] = appAliasName;
		for (const value of values) {
			const valueKey = `value${valueIndex}`;
			const query = getValueSearchFilterPart(valueKey, value, nameKey, type);
			expressionAttributeValues[`:${valueKey}`] = value;
			filterExpressionParts.push(query);
			valueIndex++;
		}
		nameIndex++;
	}
	const filterExpression = filterExpressionParts.join(' OR ');
	return {
		IndexName: APPALIAS_TENANT,
		KeyConditionExpression: 'begins_with(tenantId, :t) and appAlias_version = :a',
		ExpressionAttributeValues: expressionAttributeValues,
		FilterExpression: filterExpression,
		ExpressionAttributeNames: expressionAttributeNames
	};
}

function getValueSearchFilterPart(valueKey, value, appAliasNameKey, type) {
	// strings are contains match for partial, number/boolean has to be exact
	const valuePart = (type === 'number' || type === 'boolean') ? `#value = :${valueKey}` : `contains(#value, :${valueKey})`;
	if (!appAliasNameKey) {
		return valuePart;
	}
	return `(appAlias_name = ${appAliasNameKey} AND ${valuePart})`;
}

export function searchUserAttributeByAppAliasName(tenantId, appAlias, name, values, type, VERSION) {
	let index = 0;
	const expressionAttributeValues = {
		':t': tenantId,
		':an': `${appAlias}${TABLE_DELIMITER}${name}${TABLE_DELIMITER}${VERSION}`
	};
	const filterExpressionParts = [];
	const expressionAttributeNames = {
		'#value': "value"
	};
	index = 0;
	values.forEach((value) => {
		const valueKey = `value${index}`;
		expressionAttributeValues[`:${valueKey}`] = value;
		const filterPart = getValueSearchFilterPart(valueKey, value, null, type);
		index++;
		filterExpressionParts.push(filterPart)
	});
	let filterExpression = filterExpressionParts.join(' OR ');
	filterExpression = `(${filterExpression}) AND begins_with(tenantId,:t)`;
	return {
		IndexName: 'appAlias_name-index',
		KeyConditionExpression: 'appAlias_name = :an',
		ExpressionAttributeValues: expressionAttributeValues,
		ExpressionAttributeNames: expressionAttributeNames,
		FilterExpression: filterExpression
	};
}

export function getUsersByTenantAndApp(tenantId, appAlias) {
	return {
		TableName: TABLE_NAME,
		IndexName: 'tenantUserId-index',
		KeyConditionExpression: "tenantUserId = :an",
		FilterExpression: "appAlias = :a",
		ExpressionAttributeValues: {
			":an": `${tenantId}${TABLE_DELIMITER}`,
			":a": appAlias
		}
	}
}

export function getUserAttributesPerTenant(tenantId, userId) {
	return {
		TableName: TABLE_NAME,
		IndexName: 'tenantUserId-index',
		KeyConditionExpression: "tenantUserId = :an",
		FilterExpression: "tenantId = :t",
		ExpressionAttributeValues: {
			":an": `${tenantId}${TABLE_DELIMITER}`,
			":t": `${tenantId}${TABLE_DELIMITER}${userId}`
		}
	}
}

export function deleteAttr(payload) {
	return {
		TableName: TABLE_NAME,
		Key: {
			"tenantId": payload.tenantId,
			"appAlias_name": `${payload.appAlias_name}`
		}
	}
}

export function updateAttrPayload(payload, appAlias, VERSION, isUser = false) {
	const tenantUser = isUser ? payload.tenantId.split(TABLE_DELIMITER)[0] : false;
	const baseItem = {
		"tenantId": payload.tenantId,
		"tenantUserId": tenantUser ? `${tenantUser}${TABLE_DELIMITER}` : payload.tenantId,
		"appAlias": appAlias,
		"name": payload.name,
		"group": payload.group,
		"inToken": payload.inToken,
		"isPrivate": payload.isPrivate,
		"value": payload.value,
		... (payload.isUserManaged) && { "isUserManaged": payload.isUserManaged }
	}
	if (payload.disabled) {
		return {
			TableName: TABLE_NAME,
			Item: {
				...baseItem,
				"appAlias_group": `${appAlias}${TABLE_DELIMITER}${payload.group}`,
				"appAlias_inToken": `${appAlias}${TABLE_DELIMITER}${payload.inToken}`,
				"appAlias_version": `${appAlias}`,
				"appAlias_name": `${appAlias}${TABLE_DELIMITER}${payload.name}`
			}
		}
	}
	else {
		return {
			TableName: TABLE_NAME,
			Item: {
				...baseItem,
				"appAlias_group": `${appAlias}${TABLE_DELIMITER}${payload.group}${TABLE_DELIMITER}${VERSION}`,
				"appAlias_inToken": `${appAlias}${TABLE_DELIMITER}${payload.inToken}${TABLE_DELIMITER}${VERSION}`,
				"appAlias_version": `${appAlias}${TABLE_DELIMITER}${VERSION}`,
				"appAlias_name": `${appAlias}${TABLE_DELIMITER}${payload.name}${TABLE_DELIMITER}${VERSION}`
			}
		}
	}
}
