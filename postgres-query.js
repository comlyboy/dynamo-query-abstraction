import constants from "../../lib/constants";

const { TENANT, USER } = constants.GLOBAL_ATTRIBUTE_TYPE,
	globalAttributeDefinition = 'global_attribute_definition',
	globalAttributeDefinitionManagedBy = 'global_attribute_definition_managed_by',
	globalAttributeDefinitionSetAt = 'global_attribute_definition_set_at',
	tenantAttributeValue = 'tenant_attribute_value',
	tenantUserAttributeValue = 'tenant_user_attribute_value';

export function createTenantUserGlobalAttributesQuery(globalAttributes) {
	let tenantUserGlobalAttributeObjs = _buildCreateTenantUserGlobalAttributeObjs(globalAttributes);
	if (tenantUserGlobalAttributeObjs.size > 0) {
		return `INSERT INTO ${tenantUserAttributeValue}
            (tenant_uuid, user_uuid, global_attribute_uuid, value, created_at, created_by, updated_at, updated_by)
    VALUES
    ${tenantUserGlobalAttributeObjs.string}`;
	}
	return '';
}

export function updateTenantUserGlobalAttributesQuery(globalAttributes) {
	let updateQueries = [];
	globalAttributes.forEach(obj => {
		if (obj.tenantUserAttributeExists) {
			updateQueries.push(`UPDATE ${tenantUserAttributeValue} \
        SET value = '${obj.value}' , updated_at = CURRENT_TIMESTAMP \
        WHERE global_attribute_uuid = '${obj.uuid}' AND tenant_uuid = '${obj.tenantUuid}' AND user_uuid = '${obj.userUuid}' ;`);
		}
	}
	);
	return updateQueries;
}

export function getExistentGlobalAttributeNamesByNamesQuery(globalAttributes) {
	const names = globalAttributes.map(attr => `'${attr.name}'`).join(',');
	return `SELECT name from ${globalAttributeDefinition} WHERE ${globalAttributeDefinition}.name in (${names})`;
}

export function getGlobalAttributesByNamesQuery(globalAttributes, tenantUuid, userUuid) {
	return `SELECT ${globalAttributeDefinition} .name, ${globalAttributeDefinition}.uuid, validation, type,
    CASE  WHEN tav.tenant_uuid IS NULL THEN false ELSE true END AS tenant_user_attribute_exists,
    CASE WHEN 'USER' = ANY(array_agg(DISTINCT COALESCE(${globalAttributeDefinitionManagedBy}.managed_by, 'TENANT'))) THEN true ELSE false END AS passes_managed_by,
    CASE WHEN 'TENANTUSER' = ANY(array_agg(DISTINCT COALESCE(${globalAttributeDefinitionSetAt} .set_at, 'TENANT'))) THEN true ELSE false END AS passes_set_at
     FROM ${globalAttributeDefinition}
         LEFT JOIN ${tenantUserAttributeValue} as tav
             ON
         ${globalAttributeDefinition}.uuid = tav.global_attribute_uuid
            AND
         tav.tenant_uuid = '${tenantUuid}' and tav.user_uuid = '${userUuid}'
    LEFT JOIN ${globalAttributeDefinitionManagedBy} ON ${globalAttributeDefinition} .uuid =
    ${globalAttributeDefinitionManagedBy}.global_attribute_uuid LEFT JOIN ${globalAttributeDefinitionSetAt}
    ON ${globalAttributeDefinition} .uuid = ${globalAttributeDefinitionSetAt} .global_attribute_uuid
    WHERE ${globalAttributeDefinition} .name IN ${_getGlobalAttributeNames(globalAttributes)}
     GROUP BY ${globalAttributeDefinition} .name, ${globalAttributeDefinition} .uuid,
    tav.tenant_uuid`;
}

export function getTenantUserGlobalAttributesQuery(tenantUuid, userUuid) {
	return `SELECT ${globalAttributeDefinition}.name, ${tenantUserAttributeValue}.value, \
          ${globalAttributeDefinition}.inactive FROM ${tenantUserAttributeValue} LEFT JOIN \
          ${globalAttributeDefinition} ON ${globalAttributeDefinition} .uuid = ${tenantUserAttributeValue}.global_attribute_uuid
          WHERE ${tenantUserAttributeValue}.user_uuid = '${userUuid}'
          AND ${tenantUserAttributeValue}.tenant_uuid = '${tenantUuid}';`
}

export function getTenantUserGlobalAttributeByTenantUuidUserUuidAttributeNameQuery(tenantUuid, userUuid, attributeName) {
	return `SELECT ${globalAttributeDefinition}.name, ${tenantUserAttributeValue}.value, \
          ${globalAttributeDefinition}.inactive FROM ${tenantUserAttributeValue} LEFT JOIN \
          ${globalAttributeDefinition} ON ${globalAttributeDefinition} .uuid = ${tenantUserAttributeValue}.global_attribute_uuid
          WHERE ${tenantUserAttributeValue}.user_uuid = '${userUuid}'
          AND ${tenantUserAttributeValue}.tenant_uuid = '${tenantUuid}'
          AND ${globalAttributeDefinition}.name = '${attributeName}' ;`
}

export function createGlobalAttributesQuery(globalAttributes) {
	return `INSERT INTO ${globalAttributeDefinition} \
    (uuid, name, type, validation, inactive, created_at, created_by, updated_at, updated_by) \
    VALUES ${_buildGlobalAttributeObjs(globalAttributes)}`;
}

function _buildGlobalAttributeSetAt(globalAttributeDefinitions) {
	const valueList = globalAttributeDefinitions.flatMap(globalAttributeDefinition => {
		return globalAttributeDefinition.setAt.map(setAt => {
			return `('${globalAttributeDefinition.uuid}', '${setAt}', CURRENT_TIMESTAMP,` +
				`'${globalAttributeDefinition.createdBy}', CURRENT_TIMESTAMP, '${globalAttributeDefinition.updatedBy}')`
		});
	});
	return valueList.join(',');
}

function _buildGlobalAttributeManagedBy(globalAttributeDefinitions) {
	const valueList = globalAttributeDefinitions.flatMap(globalAttributeDefinition => {
		return globalAttributeDefinition.managedBy.map(managedBy => {
			return `('${globalAttributeDefinition.uuid}', '${managedBy}', CURRENT_TIMESTAMP,` +
				`'${globalAttributeDefinition.createdBy}', CURRENT_TIMESTAMP, '${globalAttributeDefinition.updatedBy}')`
		});
	});
	return valueList.join(',');
}

export function createGlobalAttributeSetAt(globalAttributeDefinitions) {
	return `INSERT INTO ${globalAttributeDefinitionSetAt} (global_attribute_uuid, set_at, created_at, created_by, updated_at, updated_by) ` +
		`VALUES ${_buildGlobalAttributeSetAt(globalAttributeDefinitions)}`
}

export function createGlobalAttributeManagedBy(globalAttributeDefinitions) {
	return `INSERT INTO ${globalAttributeDefinitionManagedBy} (global_attribute_uuid, managed_by, created_at, created_by, updated_at, updated_by) ` +
		`VALUES ${_buildGlobalAttributeManagedBy(globalAttributeDefinitions)}`
}

export function updateGlobalAttributeQuery(attributeName, updateGlobalAttribute) {
	const columnUpdates = _buildUpdateGlobalAttributeColumnUpdates(updateGlobalAttribute);
	return `UPDATE ${globalAttributeDefinition}\
          SET ${columnUpdates} WHERE name = '${attributeName}'\
          RETURNING created_at, updated_at, type, validation, inactive`;
}

export function findGlobalAttributesByName(attributeDefinitions) {
	const names = attributeDefinitions.map(attr => `'${attr.name}'`).join(',');
	return `SELECT ${globalAttributeDefinition}.name, ${globalAttributeDefinition}.updated_at, ` +
		`${globalAttributeDefinition}.created_at, array_replace(array_agg(DISTINCT ${globalAttributeDefinitionManagedBy}.managed_by), ` +
		`NULL, \'TENANT\') as managed_by, array_replace(array_agg(DISTINCT ${globalAttributeDefinitionSetAt}.set_at), ` +
		`NULL, \'TENANT\') as set_at FROM ${globalAttributeDefinition} left join ${globalAttributeDefinitionManagedBy} ` +
		`on ${globalAttributeDefinition}.uuid = ${globalAttributeDefinitionManagedBy}.global_attribute_uuid left join ` +
		`${globalAttributeDefinitionSetAt} on ${globalAttributeDefinition}.uuid = ${globalAttributeDefinitionSetAt}.global_attribute_uuid ` +
		`WHERE ${globalAttributeDefinition}.name in (${names}) GROUP BY ${globalAttributeDefinition}.uuid `;
}

export function findTenantUserAttributeGlobalAttributesByName(attributeDefinitions, tenantUuid, userUuid) {
	const names = attributeDefinitions.map(attr => `'${attr.name}'`).join(',');
	return `SELECT name, value, inactive FROM ${globalAttributeDefinition} LEFT JOIN
      ${tenantUserAttributeValue} ON ${globalAttributeDefinition}.uuid  = ${tenantUserAttributeValue}.global_attribute_uuid
           WHERE ${tenantUserAttributeValue}.tenant_uuid = '${tenantUuid}' AND ${tenantUserAttributeValue}.user_uuid = '${userUuid}' AND name IN (${names})`;
}
export function findGlobalAttributesQuery(limit, offset, set_at, managed_by) {
	return `WITH cte AS (SELECT ${globalAttributeDefinition}.name, ${globalAttributeDefinition}.type, NULLIF(${globalAttributeDefinition}.validation, '') as validation,
    ${globalAttributeDefinition}.inactive, ${globalAttributeDefinition}.updated_at, ${globalAttributeDefinition}.created_at,
    array_replace(array_agg(DISTINCT ${globalAttributeDefinitionManagedBy}.managed_by), NULL, 'TENANT') as managed_by,
    array_replace(array_agg(DISTINCT ${globalAttributeDefinitionSetAt}.set_at), NULL, 'TENANT') as set_at FROM  ${globalAttributeDefinition}
    LEFT JOIN ${globalAttributeDefinitionManagedBy} ON ${globalAttributeDefinition}.uuid = ${globalAttributeDefinitionManagedBy}.global_attribute_uuid
    LEFT JOIN ${globalAttributeDefinitionSetAt} ON ${globalAttributeDefinition}.uuid = ${globalAttributeDefinitionSetAt}.global_attribute_uuid GROUP BY
    ${globalAttributeDefinition}.uuid) SELECT * FROM cte WHERE EXISTS (SELECT 1 FROM unnest(cte.managed_by) as un_managed_by,
    unnest(ARRAY[${managed_by}]) as un_managed_by_check_list WHERE un_managed_by_check_list = un_managed_by) OR
    EXISTS (SELECT 1 FROM unnest(cte.set_at) as un_set_at, unnest(ARRAY[${set_at}]) as un_set_at_check_list
    WHERE un_set_at_check_list = un_set_at) ORDER BY NAME LIMIT ${limit + 1} OFFSET ${offset};`
}

export function findAttributeByNameQuery() {
	return `SELECT uuid,
                 type,
                 validation,
                 inactive,
                 COALESCE(${globalAttributeDefinitionManagedBy}.managed_by, 'TENANT') as managedBy,
                 COALESCE(${globalAttributeDefinitionSetAt}.set_at, 'TENANT') as setAt
          FROM ${globalAttributeDefinition} ` +
		`LEFT JOIN ${globalAttributeDefinitionManagedBy} ON ${globalAttributeDefinition}.uuid = ${globalAttributeDefinitionManagedBy}.global_attribute_uuid ` +
		`LEFT JOIN ${globalAttributeDefinitionSetAt} ON ${globalAttributeDefinition}.uuid = ${globalAttributeDefinitionSetAt}.global_attribute_uuid ` +
		`WHERE ${globalAttributeDefinition}.name = $1;`;
}


export function findFullGlobalAttributeDefinitionByNameQuery(attributeName) {
	return `SELECT name, type, validation, inactive \
          FROM ${globalAttributeDefinition} WHERE name='${attributeName}';`;
}

export function findTenantAttributeValuesByTenantUuidQuery(inactive) {
	return `SELECT gad.name, tav.value, gad.inactive FROM ${tenantAttributeValue} AS tav \
            LEFT JOIN ${globalAttributeDefinition} AS gad ON tav.global_attribute_uuid = gad.uuid \
            WHERE tav.tenant_uuid = $1`;
}

export function findAllActiveAttributeValuesForTenantAndUserPQuery() {
	return `SELECT gad.name, COALESCE(tuav.value, tav.value) AS value
       FROM global_attribute_definition gad LEFT JOIN tenant_attribute_value tav
         ON gad.uuid = tav.global_attribute_uuid AND tav.tenant_uuid = $1
       LEFT JOIN tenant_user_attribute_value tuav
         ON gad.uuid = tuav.global_attribute_uuid AND tuav.tenant_uuid = $1 AND tuav.user_uuid = $2
       WHERE (tav.tenant_uuid = $1 OR (tuav.tenant_uuid = $1 AND tuav.user_uuid = $2)) AND gad.inactive = FALSE;`
}


export function findAllActiveAttributeValuesForTenantAndUserAndMangedByPQuery() {
	const s = `SELECT gad.name, tuav.value, tuav.tenant_uuid as tenant_uuid, tuav.user_uuid as user_uuid FROM tenant_user_attribute_value AS tuav
        LEFT JOIN ${globalAttributeDefinition} AS gad ON tuav.global_attribute_uuid = gad.uuid
        LEFT JOIN ${globalAttributeDefinitionManagedBy} AS gadmb on gadmb.global_attribute_uuid = gad.uuid
    WHERE tuav.user_uuid = $2 AND tuav.tenant_uuid = $1 AND gad.inactive = false AND gadmb.managed_by = $3
        UNION
    SELECT gad.name, tav.value AS tenant_value, tav.tenant_uuid as tenant_uuid, null as user_uuid FROM tenant_attribute_value AS tav
        LEFT JOIN global_attribute_definition AS gad ON tav.global_attribute_uuid = gad.uuid
        LEFT JOIN ${globalAttributeDefinitionManagedBy} as gadmb on gadmb.global_attribute_uuid = gad.uuid
    WHERE tav.tenant_uuid = $1 AND gad.inactive = FALSE AND gadmb.managed_by = $3`;
	return s;
}

export function findManagedByTenantAndUserAttributesPQuery() {
	return `SELECT gad.name, tav.value, gad.inactive FROM ${tenantAttributeValue} AS tav
            INNER JOIN ${globalAttributeDefinition} AS gad ON tav.global_attribute_uuid = gad.uuid
            INNER JOIN ${globalAttributeDefinitionManagedBy} gman on gad.uuid = gman.global_attribute_uuid
          WHERE tav.tenant_uuid = $1 AND gman.managed_by = 'SELF'`;
}

export function findTenantAttributeValuesByTenantUuidAndAttributeNameQuery(inactive) {
	const inactiveSql = inactive !== undefined ? `gad.inactive = ${inactive} AND` : '';
	return `SELECT gad.name, tav.value FROM ${tenantAttributeValue} AS tav \
            LEFT JOIN ${globalAttributeDefinition} AS gad ON tav.global_attribute_uuid = gad.uuid \
            WHERE ${inactiveSql} tav.tenant_uuid = $1 AND gad.name = $2`;
}

export function findGlobalAttributeByName() {
	return `SELECT gad.uuid, gset.set_at, gman.managed_by, gad.name, gad.inactive FROM ${globalAttributeDefinition} gad \
             INNER JOIN ${globalAttributeDefinitionSetAt} gset on  gad.uuid = gset.global_attribute_uuid \
             INNER JOIN ${globalAttributeDefinitionManagedBy} gman on gset.global_attribute_uuid = gman.global_attribute_uuid \
          WHERE gad.name=$1 `;
}

export function saveTenantAttributeValuesQuery(valObjs) {
	return `INSERT INTO ${tenantAttributeValue} ("tenant_uuid", "global_attribute_uuid", "value", "created_by", "updated_by") \
            VALUES ${_buildTenantAttributeValues(valObjs)} \
                ON CONFLICT (tenant_uuid, global_attribute_uuid) \
                    DO UPDATE SET value = EXCLUDED.value, \
                        updated_by = EXCLUDED.updated_by, \
                        updated_at = CURRENT_TIMESTAMP;`;
}

export function saveTenantUserAttributeValuesQuery(valObjs) {
	return `INSERT INTO ${tenantUserAttributeValue} ("tenant_uuid", "user_uuid", "global_attribute_uuid", "value", "created_by", "updated_by") \
            VALUES ${_buildTenantUserAttributeValues(valObjs)} \
                ON CONFLICT (tenant_uuid, user_uuid, global_attribute_uuid) \
                    DO UPDATE SET value = EXCLUDED.value, \
                        updated_by = EXCLUDED.updated_by, \
                        updated_at = CURRENT_TIMESTAMP;`;
}

export function deleteTenantAttributeValueQuery() {
	return `DELETE FROM ${tenantAttributeValue} WHERE tenant_uuid = $1 AND global_attribute_uuid = $2;`
}

export function deleteTenantUserGlobalAttributeFromDB() {
	return `DELETE FROM ${tenantUserAttributeValue} WHERE tenant_uuid = $1 AND user_uuid = $2 AND global_attribute_uuid = $3;`
}

export function deleteAssociatedTenantUserGlobalAttributeFromDB() {
	return `DELETE FROM ${tenantUserAttributeValue} WHERE tenant_uuid = $1 AND user_uuid = $2;`
}

export function getTenantUserGlobalAttributeFromDB() {
	return `SELECT FROM ${tenantUserAttributeValue} WHERE tenant_uuid = $1 AND user_uuid = $2 AND global_attribute_uuid = $3;`
}
export function deleteALlTenantAttributeValuesQuery() {
	return `DELETE FROM ${tenantAttributeValue} WHERE tenant_uuid = $1;`
}

export function deleteAllGlobalAttributesForAllUsersAssociatedWithTenantFromDB() {
	return `DELETE FROM ${tenantUserAttributeValue} WHERE tenant_uuid = $1;`
}

function _buildTenantAttributeValues(valObjs) {
	const valParts = valObjs.map(valObj => _buildAttributeValue(TENANT, valObj));
	return valParts.join(',');
}

function _buildTenantUserAttributeValues(valObjs) {
	const valParts = valObjs.map(valObj => _buildAttributeValue(USER, valObj));
	return valParts.join(',');
}

function _buildUpdateGlobalAttributeColumnUpdates(updateGlobalAttributeBody) {
	let columnUpdates = [];
	if (updateGlobalAttributeBody.validation !== undefined) {
		if (updateGlobalAttributeBody.validation === null) {
			// allow to reset validation value
			columnUpdates.push(`validation=${updateGlobalAttributeBody.validation}`);
		} else {
			columnUpdates.push(`validation='${updateGlobalAttributeBody.validation}'`);
		}
	}
	if (updateGlobalAttributeBody.inactive !== undefined) {
		columnUpdates.push(`inactive=${updateGlobalAttributeBody.inactive}`);
	}
	columnUpdates.push('updated_at=CURRENT_TIMESTAMP')
	return columnUpdates.join(',');
}

function _buildAttributeValue(attributeType, valObj) {
	switch (attributeType) {
		case TENANT:
			return `('${valObj.tenantUuid}', '${valObj.attributeUuid}', '${valObj.value}', '${valObj.createdBy}', '${valObj.updatedBy}')`;
		case USER:
			return `('${valObj.tenantUuid}', '${valObj.userUuid}', '${valObj.attributeUuid}', '${valObj.value}', '${valObj.createdBy}', '${valObj.updatedBy}')`;
		default:
			return ''
	}
}

function _getGlobalAttributeNames(globalAttributeObjs) {
	const nameList = globalAttributeObjs.map(obj => `'${obj.name}'`);
	return `(${nameList.join(',')})`;
}

function _buildGlobalAttributeObjs(globalAttributeObjs) {
	const valueList = globalAttributeObjs.map(obj => {
		return obj.validation === null ?
			`('${obj.uuid}', '${obj.name}', '${obj.type}', null,\
     ${obj.inactive}, CURRENT_TIMESTAMP, '${obj.createdBy}', CURRENT_TIMESTAMP, '${obj.updatedBy}')` :
			`('${obj.uuid}', '${obj.name}', '${obj.type}', '${obj.validation}',\
     ${obj.inactive}, CURRENT_TIMESTAMP, '${obj.createdBy}', CURRENT_TIMESTAMP, '${obj.updatedBy}')`;
	});
	return valueList.join(',');
}

function _buildCreateTenantUserGlobalAttributeObjs(globalAttributeObjs) {
	let insertArray = [];
	globalAttributeObjs.forEach(obj => {
		if (!obj.tenantUserAttributeExists) {
			insertArray.push(`('${obj.tenantUuid}', '${obj.userUuid}', '${obj.uuid}', '${obj.value}',\
        CURRENT_TIMESTAMP, '${obj.createdBy}', CURRENT_TIMESTAMP, '${obj.updatedBy}')`);
		}
	}
	);
	return { string: insertArray.join(','), size: insertArray.length };
}

export function getTenantUserGlobalAttributesByTenantUuidAndUserUuidsAndMangedByQuery(tenantUuid, userUuids, managedBy) {
	const userUuidsString = userUuids.join("','");
	return `SELECT ${tenantUserAttributeValue}.user_uuid,
                 ${globalAttributeDefinition}.name,
                 ${tenantUserAttributeValue}.value,
                 ${globalAttributeDefinition}.inactive
          FROM ${tenantUserAttributeValue}
            LEFT JOIN ${globalAttributeDefinition} ON ${globalAttributeDefinition}.uuid = ${tenantUserAttributeValue}.global_attribute_uuid
            LEFT JOIN ${globalAttributeDefinitionManagedBy} ON ${globalAttributeDefinitionManagedBy}.global_attribute_uuid = ${tenantUserAttributeValue}.global_attribute_uuid
          WHERE ${tenantUserAttributeValue}.user_uuid IN ('${userUuidsString}')
          AND ${tenantUserAttributeValue}.tenant_uuid = '${tenantUuid}'
          AND ${globalAttributeDefinitionManagedBy}.managed_by = '${managedBy}';`
}
