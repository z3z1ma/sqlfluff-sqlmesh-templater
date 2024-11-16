from textwrap import dedent

from sqlfluff_templater_sqlmesh.templater import _process_sql_script


def test_sanity():
    # Ensure that the templater is working as expected.
    input_sql = """
    /* Serves as a silver table for IDP active developers. */
    MODEL (
        name "silver"."active_developers",
        kind VIEW,
        owner 'PL',
        tags array[silver, active_developers, idp],
        grains (active_developer_id, concat(account_id, project_id, user_id)),
        audits array[not_null(columns := array[account_id, user_id]), unique_values(columns := array[active_developer_id])],
        enabled @feature_flag('MODULE_IDP')
    );

    SELECT
        @generate_surrogate_key(
            account_id,
            project_id,
            user_id
        ) AS active_developer_id,
        account_id,
        org_id,
        project_id,
        user_id,
        created_time,
        last_updated_time,
        last_accessed_time,
        email, /* The hashed email of the developer */
        user_name, /* The username of the developer */
        is_deleted
    FROM "staging"."platform-harness-idp-activeDevelopers";

    VACUUM @this_model;
    """

    templated_file = _process_sql_script(
        dedent(input_sql), fname="example.sql", dialect="postgres"
    )

    assert (
        templated_file.templated_str
        == dedent("""
    SELECT
        generate_surrogate_key('PLACEHOLDER') AS active_developer_id,
        account_id,
        org_id,
        project_id,
        user_id,
        created_time,
        last_updated_time,
        last_accessed_time,
        email, /* The hashed email of the developer */
        user_name, /* The username of the developer */
        is_deleted
    FROM "staging"."platform-harness-idp-activeDevelopers"
    """).strip()
    )
