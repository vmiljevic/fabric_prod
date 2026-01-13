[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "time_card_create_table_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared time_card_create_table = let
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
shared time_card_create_table_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data],
  TableNavigation = Navigation_2{[Id = "time_card", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
