[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "time_card_delete_rows_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "u_project_name", DestinationColumnName = "u_project_name"], [SourceColumnName = "top_task", DestinationColumnName = "top_task"], [SourceColumnName = "task", DestinationColumnName = "task"], [SourceColumnName = "u_activity_cluster", DestinationColumnName = "u_activity_cluster"], [SourceColumnName = "total", DestinationColumnName = "total"], [SourceColumnName = "week_starts_on", DestinationColumnName = "week_starts_on"], [SourceColumnName = "monday", DestinationColumnName = "monday"], [SourceColumnName = "tuesday", DestinationColumnName = "tuesday"], [SourceColumnName = "wednesday", DestinationColumnName = "wednesday"], [SourceColumnName = "thursday", DestinationColumnName = "thursday"], [SourceColumnName = "friday", DestinationColumnName = "friday"], [SourceColumnName = "saturday", DestinationColumnName = "saturday"], [SourceColumnName = "sunday", DestinationColumnName = "sunday"], [SourceColumnName = "category", DestinationColumnName = "category"], [SourceColumnName = "state", DestinationColumnName = "state"], [SourceColumnName = "u_dept_cost_center", DestinationColumnName = "u_dept_cost_center"], [SourceColumnName = "u_prj_unit_resp", DestinationColumnName = "u_prj_unit_resp"], [SourceColumnName = "u_rep_project", DestinationColumnName = "u_rep_project"], [SourceColumnName = "u_timecard_source", DestinationColumnName = "u_timecard_source"], [SourceColumnName = "u_rate_role", DestinationColumnName = "u_rate_role"], [SourceColumnName = "user_name", DestinationColumnName = "user_name"], [SourceColumnName = "u_project_manager", DestinationColumnName = "u_project_manager"], [SourceColumnName = "u_resource_manager", DestinationColumnName = "u_resource_manager"], [SourceColumnName = "resource_plan", DestinationColumnName = "resource_plan"], [SourceColumnName = "user", DestinationColumnName = "user"], [SourceColumnName = "sys_id", DestinationColumnName = "sys_id"], [SourceColumnName = "sys_updated_by", DestinationColumnName = "sys_updated_by"], [SourceColumnName = "sys_updated_on", DestinationColumnName = "sys_updated_on"], [SourceColumnName = "resource_plan_name", DestinationColumnName = "resource_plan_name"], [SourceColumnName = "task_name", DestinationColumnName = "task_name"], [SourceColumnName = "project_number", DestinationColumnName = "project_number"], [SourceColumnName = "project_sys_id", DestinationColumnName = "project_sys_id"], [SourceColumnName = "user_sys_id", DestinationColumnName = "user_sys_id"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared time_card_delete_rows = let
    // === Define expected schema ===
    EmptyTable = #table(
        type table [
            u_project_name = text,
            top_task = text,
            task = text,
            u_activity_cluster = text,
            total = text,
            week_starts_on = text,
            monday = text,
            tuesday = text,
            wednesday = text,
            thursday = text,
            friday = text,
            saturday = text,
            sunday = text,
            category = text,
            state = text,
            u_dept_cost_center = text,
            u_prj_unit_resp = text,
            u_rep_project = text,
            u_timecard_source = text,
            u_rate_role = text,
            user_name = text,
            u_project_manager = text,
            u_resource_manager = text,
            resource_plan = text,
            user = text,
            sys_id = text,
            sys_updated_by = text,
            sys_updated_on = text,
            resource_plan_name = text,
            task_name = text,
            project_number = text,
            project_sys_id = text,
            user_sys_id = text
        ],
        {}
    )
in
    EmptyTable;
shared time_card_delete_rows_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data],
  TableNavigation = Navigation_2{[Id = "time_card", ItemKind = "Table"]}[Data]
in
  TableNavigation;
