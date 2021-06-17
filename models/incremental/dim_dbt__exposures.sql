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

)

select * from fields