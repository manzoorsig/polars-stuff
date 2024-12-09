select app.id, app.name,app.description, app.created_at, app.in_trash, 
      proj.id, proj.application_id, proj.name, proj.description, 
      proj.created_at, proj.in_trash , 
      br.id, br.name, br.description,br.in_trash, br.created_at
from `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_application` as app
left join `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_project` as proj on app.organization_id = proj.organization_id and app.id = proj.application_id
left join `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_branch` as br on app.id = br.organization_id and proj.id = br.project_id
order by app.id;
--31867 rows

select count(*) from 
( 
select app.id, app.name, app.created_at, app.in_trash, proj.id, proj.application_id, proj.name, proj.created_at, proj.in_trash , br.id, br.name, br.in_trash, br.created_at
from `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_application` as app
left join `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_project` as proj on app.organization_id = proj.organization_id and app.id = proj.application_id
left join `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_branch` as br on app.id = br.organization_id and proj.id = br.project_id
order by app.id
)





select count(*) from 
(SELECT app.organization_id as organization_id,app.id as application_id, 
                     app.name as application_name,app.description as application_description, 
                     app.created_at as application_created_at,app.updated_at as application_updated_at,
                     app.in_trash as application_in_trash, 
                     proj.id as project_id, proj.name as project_name, proj.description as project_description, 
                     proj.created_at as project_created_at,proj.updated_at as project_updated_at, proj.in_trash as project_in_trash,
                     proj.state as project_state, proj.entry_point_url as project_entry_point_url, -- proj.entry_point_private ,proj.proxy_type,
                     br.id as branch_id, br.name as branch_name, br.description as branch_description,br.in_trash as branch_in_trash,
                     br.created_at as branch_created_at, br.updated_at as branch_updated_at
            FROM `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_application` as app
            left join `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_project` as proj on app.organization_id = proj.organization_id and app.id = proj.application_id
            left join `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_branch` as br on app.id = br.organization_id and proj.id = br.project_id
)
-- 320397 rows


SELECT app.organization_id as organization_id,app.id as application_id, 
                     app.name as application_name,app.description as application_description, 
                     app.created_at as application_created_at,app.updated_at as application_updated_at,
                     app.in_trash as application_in_trash, 
                     proj.id as project_id, proj.name as project_name, proj.description as project_description, 
                     proj.created_at as project_created_at,proj.updated_at as project_updated_at, proj.in_trash as project_in_trash,
                     proj.state as project_state, proj.entry_point_url as project_entry_point_url, -- proj.entry_point_private ,proj.proxy_type,
                     br.id as branch_id, br.name as branch_name, br.description as branch_description,br.is_default as branch_is_default, br.in_trash as branch_in_trash,
                     br.created_at as branch_created_at, br.updated_at as branch_updated_at
            FROM `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_application` as app
            left join `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_project` as proj on app.organization_id = proj.organization_id and app.id = proj.application_id
            left join `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_branch` as br on app.organization_id = br.organization_id and proj.id = br.project_id
            
            ORDER BY application_id,project_id, branch_id
            LIMIT 100


SELECT app.organization_id as organization_id,app.id as application_id, 
                     app.name as application_name,app.description as application_description, 
                     app.created_at as application_created_at,app.updated_at as application_updated_at,
                     app.in_trash as application_in_trash, 
                     proj.id as project_id, proj.name as project_name, proj.description as project_description, 
                     proj.created_at as project_created_at,proj.updated_at as project_updated_at, proj.in_trash as project_in_trash,
                     proj.state as project_state, proj.entry_point_url as project_entry_point_url , -- proj.entry_point_private ,proj.proxy_type,
                     br.id as branch_id, br.name as branch_name, br.description as branch_description,br.is_default as is_default,
                     br.in_trash as branch_in_trash,
                     br.created_at as branch_created_at, br.updated_at as branch_updated_at
            FROM `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_application` as app
            JOIN `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_project` as proj on app.organization_id = proj.organization_id and app.id = proj.application_id
            JOIN `gcp-sig-datalake-staging.stg_sink_data.portfolio_service_branch` as br on proj.organization_id = br.organization_id and proj.id = br.project_id
            ORDER BY app.id, proj.id
   




