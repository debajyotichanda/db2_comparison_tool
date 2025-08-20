*** Settings ***
Resource    ../resources/db2_keywords.robot
Resource    ../resources/variables.robot
Library     ../db2_comparison_tool/libraries

*** Test Cases ***
Validate Data Aggregation For Table ${TABLE_NAME}
    [Documentation]    Compare aggregate statistics for numerical data
    [Tags]    data    aggregation

    ${source_data}=    Execute Query    source    SELECT * FROM ${SCHEMA_NAME}.${TABLE_NAME} SAMPLE 1000
    ${target_data}=    Execute Query    target    SELECT * FROM ${SCHEMA_NAME}.${TABLE_NAME} SAMPLE 1000

    ${numeric_columns}=    Create List    AMOUNT    QUANTITY    PRICE
    ${categorical_columns}=    Create List    STATUS    CATEGORY
    ${datetime_columns}=    Create List    CREATED_DATE    UPDATED_DATE

    ${agg_results}=    Compare Aggregates    ${source_data}    ${target_data}
    ...    ${numeric_columns}    ${categorical_columns}    ${datetime_columns}

    FOR    ${col}    IN    @{numeric_columns}
        Should Be True    ${agg_results}[${col}][matches]
        ...    msg=Aggregate mismatch for column ${col}
    END

Validate Primary Key Consistency
    [Documentation]    Validate primary key values match between source and target
    [Tags]    primary_key    validation

    ${pk_columns}=    Get Primary Key Columns    ${SCHEMA_NAME}    ${TABLE_NAME}
    ${pk_query}=    Set Variable    SELECT ${pk_columns} FROM ${SCHEMA_NAME}.${TABLE_NAME} ORDER BY ${pk_columns}

    ${source_pks}=    Execute Query    source    ${pk_query}
    ${target_pks}=    Execute Query    target    ${pk_query}

    Should Be Equal    ${source_pks}    ${target_pks}
    ...    msg=Primary key values mismatch