[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data]
in
  #"Navigation 2";
shared QueryParameters = let
  Source = Excel.Workbook(Web.Contents("https://allianzms.sharepoint.com/teams/AT0090-2759670/pbi/Shared%20Documents/01_Power%20BI/Data%20Sources/00_Master%20Data/Master%20Domain%20Data.xlsx"), null, true),
  GradeandProficiencyLevels_Table = Source{[Item = "QueryParameters", Kind = "Table"]}[Data],
  ParamQry = CompanyCode,
  #"Filtered Rows" = Table.SelectRows(GradeandProficiencyLevels_Table, each [Country] = ParamQry),
  #"Transform columns" = Table.TransformColumnTypes(#"Filtered Rows", {{"Parameters Name", type text}, {"Value", type text}, {"Country", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"Parameters Name", null}, {"Value", null}, {"Country", null}})
in
  #"Replace errors";
shared CompanyCode = "AT45" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type any];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "time_card_c_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "u_project_name", DestinationColumnName = "u_project_name"], [SourceColumnName = "task", DestinationColumnName = "task"], [SourceColumnName = "u_activity_cluster", DestinationColumnName = "u_activity_cluster"], [SourceColumnName = "total", DestinationColumnName = "total"], [SourceColumnName = "week_starts_on", DestinationColumnName = "week_starts_on"], [SourceColumnName = "monday", DestinationColumnName = "monday"], [SourceColumnName = "tuesday", DestinationColumnName = "tuesday"], [SourceColumnName = "wednesday", DestinationColumnName = "wednesday"], [SourceColumnName = "thursday", DestinationColumnName = "thursday"], [SourceColumnName = "friday", DestinationColumnName = "friday"], [SourceColumnName = "saturday", DestinationColumnName = "saturday"], [SourceColumnName = "sunday", DestinationColumnName = "sunday"], [SourceColumnName = "category", DestinationColumnName = "category"], [SourceColumnName = "state", DestinationColumnName = "state"], [SourceColumnName = "u_dept_cost_center", DestinationColumnName = "u_dept_cost_center"], [SourceColumnName = "u_prj_unit_resp", DestinationColumnName = "u_prj_unit_resp"], [SourceColumnName = "u_timecard_source", DestinationColumnName = "u_timecard_source"], [SourceColumnName = "u_rate_role", DestinationColumnName = "u_rate_role"], [SourceColumnName = "user_name", DestinationColumnName = "user_name"], [SourceColumnName = "u_project_manager", DestinationColumnName = "u_project_manager"], [SourceColumnName = "u_resource_manager", DestinationColumnName = "u_resource_manager"], [SourceColumnName = "resource_plan", DestinationColumnName = "resource_plan"], [SourceColumnName = "user", DestinationColumnName = "user"], [SourceColumnName = "sys_id", DestinationColumnName = "sys_id"], [SourceColumnName = "sys_updated_by", DestinationColumnName = "sys_updated_by"], [SourceColumnName = "sys_updated_on", DestinationColumnName = "sys_updated_on"], [SourceColumnName = "resource_plan_name", DestinationColumnName = "resource_plan_name"], [SourceColumnName = "task_name", DestinationColumnName = "task_name"], [SourceColumnName = "project_number", DestinationColumnName = "project_number"], [SourceColumnName = "project_sys_id", DestinationColumnName = "project_sys_id"], [SourceColumnName = "user_sys_id", DestinationColumnName = "user_sys_id"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared time_card_c = let
    // Parameters
    startDate    = DateWeek,
    endDate      = DateWeek,
    RecordLimit  = Number.FromText(QueryParameters{2}[Value]),
    CompanyCodes = QueryParameters{3}[Value],
    Debtor       = QueryParameters{4}[Value],

    // Ensure ISO-8601 text for ServiceNow
    startDateText = if Value.Is(startDate, type date) then Date.ToText(startDate, "yyyy-MM-dd") else Text.From(startDate),
    endDateText   = if Value.Is(endDate,   type date) then Date.ToText(endDate,   "yyyy-MM-dd") else Text.From(endDate),

    // === Single API Call (no paging) ===
    Source = Json.Document(
        Web.Contents(
            "https://aztech.service-now.com/api/now/table/time_card",
            [
                Query = [
                    sysparm_display_value = "true",
                    sysparm_exclude_reference_link = "false",
                    sysparm_fields =
                        "u_project_name,top_task,task,u_activity_cluster,total," &
                        "week_starts_on,monday,tuesday,wednesday,thursday,friday," &
                        "saturday,sunday,category,state,u_dept_cost_center,u_prj_unit_resp," &
                        "u_rep_project,u_timecard_source,u_rate_role,user.user_name," &
                        "u_project_manager,u_resource_manager,resource_plan,user,sys_id," &
                        "sys_updated_by,sys_updated_on,resource_plan.short_description," &
                        "task.short_description",
                    sysparm_query = "week_starts_onBETWEEN" & startDateText & "@" & endDateText & "^state!=Locked^total!=0^categoryNOT LIKEProject^user.user_nameISNOTEMPTY^" & "user.u_debtor.sys_id=" & Debtor & "^ORDERBYsys_created_on",
                    sysparm_limit = Text.From(RecordLimit)
                ]
            ]
        )
    ),

    // Convert to table safely
    Result = try Source[result] otherwise {},
    #"Converted to Table" = Table.FromRecords(Result),

    // === SafeTable ensures we always have structure ===
    SafeTable =
        if Table.IsEmpty(#"Converted to Table") then
            #table(
                {
                    "u_project_name","top_task","task","u_activity_cluster","total",
                    "week_starts_on","monday","tuesday","wednesday","thursday","friday",
                    "saturday","sunday","category","state","u_dept_cost_center","u_prj_unit_resp",
                    "u_rep_project","u_timecard_source","u_rate_role","user.user_name",
                    "u_project_manager","u_resource_manager","resource_plan","user","sys_id",
                    "sys_updated_by","sys_updated_on","resource_plan.short_description",
                    "task.short_description"
                },
                {}
            )
        else
            #"Converted to Table",

    // === Helper for safe text coercion ===
    EnsureText = (t as table, col as text) as table =>
        if Table.HasColumns(t, col) then
            Table.TransformColumns(t, {{col, each try Text.From(_) otherwise null, type text}})
        else
            t,

    // === Expansion / Cleanup Steps with guards ===
    #"Expanded user"               = try Table.ExpandRecordColumn(SafeTable, "user", {"display_value","link"}, {"display_value","link"}) otherwise SafeTable,
    #"Expanded u_prj_unit_resp"    = try Table.ExpandRecordColumn(#"Expanded user", "u_prj_unit_resp", {"display_value","link"}, {"display_value.1","link.1"}) otherwise #"Expanded user",
    #"Expanded top_task"           = try Table.ExpandRecordColumn(#"Expanded u_prj_unit_resp", "top_task", {"display_value","link"}, {"display_value.2","link.2"}) otherwise #"Expanded u_prj_unit_resp",
    #"Expanded u_resource_manager" = try Table.ExpandRecordColumn(#"Expanded top_task", "u_resource_manager", {"display_value","link"}, {"display_value.3","link.3"}) otherwise #"Expanded top_task",
    #"Expanded task"               = try Table.ExpandRecordColumn(#"Expanded u_resource_manager", "task", {"display_value","link"}, {"display_value.4","link.4"}) otherwise #"Expanded u_resource_manager",
    #"Expanded u_dept_cost_center" = try Table.ExpandRecordColumn(#"Expanded task", "u_dept_cost_center", {"display_value","link"}, {"display_value.5","link.5"}) otherwise #"Expanded task",
    #"Renamed columns"             = Table.RenameColumns(#"Expanded u_dept_cost_center", {{"display_value.2","project_number"}}, MissingField.Ignore),
    #"Expanded u_rep_project"      = try Table.ExpandRecordColumn(#"Renamed columns", "u_rep_project", {"display_value","link"}, {"display_value.2","link.6"}) otherwise #"Renamed columns",
    #"Removed columns"             = Table.RemoveColumns(#"Expanded u_rep_project", {"link.2"}, MissingField.Ignore),
    #"Renamed columns 1"           = Table.RenameColumns(#"Removed columns", {{"display_value.1","u_prj_unit_resp"}}, MissingField.Ignore),
    #"Removed columns 1"           = Table.RemoveColumns(#"Renamed columns 1", {"link.1","link.5"}, MissingField.Ignore),
    #"Renamed columns 2"           = Table.RenameColumns(#"Removed columns 1", {{"display_value.5","u_dept_cost_center"},{"user.user_name","user_name"}}, MissingField.Ignore),

    // --- Handle project_sys_id safely ---
    AfterText6 = EnsureText(#"Renamed columns 2", "link.6"),
    #"Split column by delimiter" =
        if Table.HasColumns(AfterText6, "link.6") then
            Table.SplitColumn(
                AfterText6,
                "link.6",
                Splitter.SplitTextByEachDelimiter({"/"}, QuoteStyle.Csv, true),
                {"link.7","link.8"}
            )
        else
            AfterText6,

    #"Removed columns 2" =
        if Table.HasColumns(#"Split column by delimiter", "link.7") then
            Table.RemoveColumns(#"Split column by delimiter", {"link.7"}, MissingField.Ignore)
        else
            #"Split column by delimiter",

    #"Renamed columns 3" =
        if Table.HasColumns(#"Removed columns 2", "link.8") then
            Table.RenameColumns(#"Removed columns 2", {{"link.8","project_sys_id"}}, MissingField.Ignore)
        else
            #"Removed columns 2",

    #"Expanded u_project_manager"  = try Table.ExpandRecordColumn(#"Renamed columns 3", "u_project_manager", {"display_value","link"}, {"display_value.1","link.1"}) otherwise #"Renamed columns 3",
    #"Renamed columns 4"           = Table.RenameColumns(#"Expanded u_project_manager", {{"display_value.1","u_project_manager"}}, MissingField.Ignore),
    #"Removed columns 3"           = Table.RemoveColumns(#"Renamed columns 4", {"link.1"}, MissingField.Ignore),
    #"Renamed columns 5"           = Table.RenameColumns(#"Removed columns 3", {{"display_value.3","u_resource_manager"}}, MissingField.Ignore),
    #"Removed columns 4"           = Table.RemoveColumns(#"Renamed columns 5", {"link.3"}, MissingField.Ignore),
    #"Renamed columns 6"           = Table.RenameColumns(#"Removed columns 4", {{"display_value.4","task"}}, MissingField.Ignore),
    #"Removed columns 5"           = Table.RemoveColumns(#"Renamed columns 6", {"link.4"}, MissingField.Ignore),
    #"Renamed columns 7"           = Table.RenameColumns(#"Removed columns 5", {{"display_value","user"}}, MissingField.Ignore),

    // --- Handle user_sys_id safely ---
    AfterTextLink = EnsureText(#"Renamed columns 7", "link"),
    #"Split column by delimiter 1" =
        if Table.HasColumns(AfterTextLink, "link") then
            Table.SplitColumn(
                AfterTextLink,
                "link",
                Splitter.SplitTextByEachDelimiter({"/"}, QuoteStyle.Csv, true),
                {"link.1","link.2"}
            )
        else
            AfterTextLink,

    #"Removed columns 6"  = Table.RemoveColumns(#"Split column by delimiter 1", {"link.1"}, MissingField.Ignore),
    #"Renamed columns 8"  = Table.RenameColumns(#"Removed columns 6", {{"link.2","user_sys_id"}}, MissingField.Ignore),

    #"Expanded resource_plan"      = try Table.ExpandRecordColumn(#"Renamed columns 8", "resource_plan", {"display_value","link"}, {"display_value","link"}) otherwise #"Renamed columns 8",
    #"Renamed columns 9"           = Table.RenameColumns(#"Expanded resource_plan", {{"display_value","resource_plan"}}, MissingField.Ignore),
    #"Removed columns 7"           = Table.RemoveColumns(#"Renamed columns 9", {"display_value.2","link"}, MissingField.Ignore),

    // Ensure all expected columns exist
    ExpectedColumns = {
        "u_project_name","saturday","project_number","u_prj_unit_resp","u_rate_role","u_timecard_source",
        "sys_updated_on","user_name","u_dept_cost_center","sys_id","total","sys_updated_by",
        "resource_plan.short_description","week_starts_on","wednesday","friday","state","resource_plan",
        "project_sys_id","u_project_manager","monday","thursday","sunday","u_resource_manager","task",
        "tuesday","task.short_description","category","user","user_sys_id","u_activity_cluster"
    },
    AddMissingColumns =
        List.Accumulate(
            ExpectedColumns,
            #"Removed columns 7",
            (t, c) => if Table.HasColumns(t, c) then t else Table.AddColumn(t, c, each null)
        ),

    // Replace Errors
    #"Replaced errors" = Table.ReplaceErrorValues(
        AddMissingColumns,
        List.Transform(ExpectedColumns, each {_, ""})
    ),

    // Column Types
    #"Changed column type" = Table.TransformColumnTypes(
        #"Replaced errors",
        List.Transform(ExpectedColumns, each {_, type text})
    ),
  #"Renamed columns 10" = Table.RenameColumns(#"Changed column type", {{"resource_plan.short_description", "resource_plan_name"}, {"task.short_description", "task_name"}})
in
    #"Renamed columns 10";
[BindToDefaultDestination = true]
shared DateWeek = "2024-12-29" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];
shared time_card_c_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data],
  TableNavigation = Navigation_2{[Id = "time_card", ItemKind = "Table"]}[Data]
in
  TableNavigation;
