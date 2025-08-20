# resources/db2_keywords.robot
*** Settings ***
Library    ../libraries/db2_connector.py    WITH NAME    DB2Connector
Library    ../libraries/db2_comparison.py    WITH NAME    DB2Comparison
Library    Collections

*** Variables ***
${SCHEMA_QUERY}    SELECT COLNAME, TYPENAME, LENGTH, SCALE, NULLS, DEFAULT, COLNO
...                FROM SYSCAT.COLUMNS
...                WHERE TABNAME = UPPER('${TABLE_NAME}')
...                AND TABSCHEMA = UPPER('${SCHEMA_NAME}')
...                ORDER BY COLNO

${ROW_COUNT_QUERY}    SELECT COUNT(*) AS ROW_COUNT FROM ${SCHEMA_NAME}.${TABLE_NAME}

*** Keywords ***
Connect To DB2 Sources
    [Arguments]    ${source_config}    ${target_config}
    DB2Connector.Connect To DB2    source    ${source_config}[host]    ${source_config}[port]
    ...              ${source_config}[database]    ${source_config}[username]    ${source_config}[password]
    DB2Connector.Connect To DB2    target    ${target_config}[host]    ${target_config}[port]
    ...              ${target_config}[database]    ${target_config}[username]    ${target_config}[password]

Get Table Schema
    [Arguments]    ${connection_name}    ${schema_name}    ${table_name}
    ${query}=    Set Variable    ${SCHEMA_QUERY.replace('${TABLE_NAME}', ${table_name}).replace('${SCHEMA_NAME}', ${schema_name})}
    ${result}=    DB2Connector.Execute Query    ${connection_name}    ${query}
    [Return]    ${result}

Get Row Count
    [Arguments]    ${connection_name}    ${schema_name}    ${table_name}
    ${query}=    Set Variable    ${ROW_COUNT_QUERY.replace('${SCHEMA_NAME}', ${schema_name}).replace('${TABLE_NAME}', ${table_name})}
    ${result}=    DB2Connector.Execute Query    ${connection_name}    ${query}
    [Return]    ${result}[0][ROW_COUNT]