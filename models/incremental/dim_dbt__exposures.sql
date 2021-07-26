{{
  config(
    materialized='incremental',
    unique_key='manifest_model_id'
    )
}}

with dbt_models as (

    select * from {{ ref('stg_dbt__exposures') }}

),

dbt_models_incremental as (

    select *
    from dbt_models

    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where artifact_generated_at > (select max(artifact_generated_at) from {{ this }})
    {% endif %}

),

fields as (

<<<<<<< HEAD
     select
        t.manifest_model_id,
        t.command_invocation_id,
        t.dbt_cloud_run_id,
        t.artifact_generated_at,
        t.node_id,
        t.name,
        t.type,
        t.owner,
        t.maturity,
        f.value::string as output_feeds,
        t.package_name
    from dbt_models_incremental as t,
    lateral flatten(input => depends_on_nodes) as f
=======
     select 
        manifest_model_id,
        command_invocation_id,
        artifact_generated_at,
        node_id,
        name,
        type,
        owner,
        maturity,
        depends_on_nodes,
        depends_on_sources,
        package_name
    from dbt_models_incremental
>>>>>>> 5a6836ac8b0db1b7e51a2fa99a3295813aa27a89

)

select * from fields