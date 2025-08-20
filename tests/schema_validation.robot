# tests/schema_validation.robot
*** Settings ***
Resource    ../resources/db2_keywords.robot
Resource    ../resources/variables.robot
Suite Setup    Connect To DB2 Sources    ${SOURCE_CONFIG}    ${TARGET_CONFIG}
Suite Teardown    Disconnect All

*** Test Cases ***
Validate Schema For Table ${TABLE_NAME}
    [Documentation]    Validate schema comparison between source and target DB2
    [Tags]    schema    validation

    ${source_schema}=    Get Table Schema    source    ${SCHEMA_NAME}    ${TABLE_NAME}
    ${target_schema}=    Get Table Schema    target    ${SCHEMA_NAME}    ${TABLE_NAME}

    ${comparison_result}=    Compare Table Schemas    ${source_schema}    ${target_schema}    ${TABLE_NAME}

    Log    Comparison Results: ${comparison_result}
    Validate Schema Comparison    ${comparison_result}

Validate Row Count For Table ${TABLE_NAME}
    [Documentation]    Validate row count matches between source and target
    [Tags]    row_count    validation

    ${source_count}=    Get Row Count    source    ${SCHEMA_NAME}    ${TABLE_NAME}
    ${target_count}=    Get Row Count    target    ${SCHEMA_NAME}    ${TABLE_NAME}

    ${count_result}=    Compare Row Counts    ${source_count}    ${target_count}
    Log    Row count validation passed: ${count_result}