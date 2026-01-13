[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared DefaultDestination = let
  Source = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data]
in
  #"Navigation 2";
shared time_card_a = let
    // Parameters
    startDate   = Date.FromText(QueryParameters{0}[Value]),
    endDate     = Date.FromText(QueryParameters{1}[Value]),
    PageSize    = Number.FromText(QueryParameters{2}[Value]),
    CompanyCodes= QueryParameters{3}[Value],
    Debtor      = QueryParameters{4}[Value],

    // Function to fetch one page
    GetPage = (Offset as number) =>
        let
            Response = Json.Document(
                Web.Contents(
                    "https://aztech.service-now.com/api/now/table/time_card",
                    [
                        Query = [
                            sysparm_display_value="true",
                            sysparm_exclude_reference_link="false",
                            sysparm_fields="u_project_name,top_task,task,u_activity_cluster,total,week_starts_on,monday,tuesday,wednesday,thursday,friday,saturday,sunday,category,state,u_dept_cost_center,u_prj_unit_resp,u_rep_project,u_timecard_source,u_rate_role,user.user_name,u_project_manager,u_resource_manager,resource_plan,user,sys_id,sys_updated_by,sys_updated_on,resource_plan.short_description,task.short_description",
                            sysparm_query=
                                "week_starts_onBETWEEN" & Date.ToText(startDate, "yyyy-MM-dd") & 
                                "@" & Date.ToText(endDate, "yyyy-MM-dd") & 
                                "^state!=Locked^total!=0^categorySTARTSWITHProject^top_task.ref_pm_project.u_accounting_master.u_master_approved!=Rejected^top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeIN" & CompanyCodes,
                            sysparm_limit = Number.ToText(PageSize),
                            sysparm_offset = Number.ToText(Offset)
                        ],
                        Headers=[Accept="application/json"],
                        Timeout=#duration(0,0,30,0)
                    ]
                )
            ),
            Result = try Response[result] otherwise {}
        in
            Result,

    // Keep calling pages until no rows returned
    Pages = List.Generate(
        () => [Offset = 0, Page = GetPage(0)],
        each List.Count([Page]) > 0,
        each [Offset = [Offset] + PageSize, Page = GetPage([Offset])],
        each [Page]
    ),

    // Flatten all pages
    AllRows = List.Combine(Pages),
    #"Converted to Table" = Table.FromList(AllRows, Splitter.SplitByNothing(), {"result"}),

    // === Your full transformation logic ===
    #"Expanded result" = Table.ExpandRecordColumn(#"Converted to Table", "result", {"u_project_name", "saturday", "top_task", "u_prj_unit_resp", "u_rate_role", "u_timecard_source", "sys_updated_on", "user.user_name", "u_dept_cost_center", "sys_id", "total", "sys_updated_by", "resource_plan.short_description", "week_starts_on", "wednesday", "friday", "state", "resource_plan", "u_rep_project", "u_project_manager", "monday", "thursday", "sunday", "u_resource_manager", "task", "tuesday", "task.short_description", "category", "user", "u_activity_cluster"}, {"u_project_name", "saturday", "top_task", "u_prj_unit_resp", "u_rate_role", "u_timecard_source", "sys_updated_on", "user.user_name", "u_dept_cost_center", "sys_id", "total", "sys_updated_by", "resource_plan.short_description", "week_starts_on", "wednesday", "friday", "state", "resource_plan", "u_rep_project", "u_project_manager", "monday", "thursday", "sunday", "u_resource_manager", "task", "tuesday", "task.short_description", "category", "user", "u_activity_cluster"}),

    #"Expanded user" = Table.ExpandRecordColumn(#"Expanded result", "user", {"display_value", "link"}, {"display_value", "link"}),
    #"Expanded u_prj_unit_resp" = Table.ExpandRecordColumn(#"Expanded user", "u_prj_unit_resp", {"display_value", "link"}, {"display_value.1", "link.1"}),
    #"Expanded top_task" = Table.ExpandRecordColumn(#"Expanded u_prj_unit_resp", "top_task", {"display_value", "link"}, {"display_value.2", "link.2"}),
    #"Expanded u_resource_manager" = Table.ExpandRecordColumn(#"Expanded top_task", "u_resource_manager", {"display_value", "link"}, {"display_value.3", "link.3"}),
    #"Expanded task" = Table.ExpandRecordColumn(#"Expanded u_resource_manager", "task", {"display_value", "link"}, {"display_value.4", "link.4"}),
    #"Expanded u_dept_cost_center" = Table.ExpandRecordColumn(#"Expanded task", "u_dept_cost_center", {"display_value", "link"}, {"display_value.5", "link.5"}),

    #"Renamed columns" = Table.RenameColumns(#"Expanded u_dept_cost_center", {{"display_value.2", "project_number"}}),
    #"Expanded u_rep_project" = Table.ExpandRecordColumn(#"Renamed columns", "u_rep_project", {"display_value", "link"}, {"display_value.2", "link.6"}),
    #"Removed columns" = Table.RemoveColumns(#"Expanded u_rep_project", {"link.2"}),
    #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"display_value.1", "u_prj_unit_resp"}}),
    #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 1", {"link.1", "link.5"}),
    #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"display_value.5", "u_dept_cost_center"}, {"user.user_name", "user_name"}}),
    #"Split column by delimiter" = Table.SplitColumn(Table.TransformColumnTypes(#"Renamed columns 2", {{"link.6", type text}}), "link.6", Splitter.SplitTextByEachDelimiter({"/"}, QuoteStyle.Csv, true), {"link.7", "link.8"}),
    #"Removed columns 2" = Table.RemoveColumns(#"Split column by delimiter", {"link.7"}),
    #"Renamed columns 3" = Table.RenameColumns(#"Removed columns 2", {{"link.8", "project_sys_id"}}),
    #"Expanded u_project_manager" = Table.ExpandRecordColumn(#"Renamed columns 3", "u_project_manager", {"display_value", "link"}, {"display_value.1", "link.1"}),
    #"Renamed columns 4" = Table.RenameColumns(#"Expanded u_project_manager", {{"display_value.1", "u_project_manager"}}),
    #"Removed columns 3" = Table.RemoveColumns(#"Renamed columns 4", {"link.1"}),
    #"Renamed columns 5" = Table.RenameColumns(#"Removed columns 3", {{"display_value.3", "u_resource_manager"}}),
    #"Removed columns 4" = Table.RemoveColumns(#"Renamed columns 5", {"link.3"}),
    #"Renamed columns 6" = Table.RenameColumns(#"Removed columns 4", {{"display_value.4", "task"}}),
    #"Removed columns 5" = Table.RemoveColumns(#"Renamed columns 6", {"link.4"}),
    #"Renamed columns 7" = Table.RenameColumns(#"Removed columns 5", {{"display_value", "user"}}),
    #"Split column by delimiter 1" = Table.SplitColumn(Table.TransformColumnTypes(#"Renamed columns 7", {{"link", type text}}), "link", Splitter.SplitTextByEachDelimiter({"/"}, QuoteStyle.Csv, true), {"link.1", "link.2"}),
    #"Removed columns 6" = Table.RemoveColumns(#"Split column by delimiter 1", {"link.1"}),
    #"Renamed columns 8" = Table.RenameColumns(#"Removed columns 6", {{"link.2", "user_sys_id"}}),
    #"Expanded resource_plan" = Table.ExpandRecordColumn(#"Renamed columns 8", "resource_plan", {"display_value", "link"}, {"display_value", "link"}),
    #"Renamed columns 9" = Table.RenameColumns(#"Expanded resource_plan", {{"display_value", "resource_plan"}}),
    #"Removed columns 7" = Table.RemoveColumns(#"Renamed columns 9", {"display_value.2", "link"}),

    #"Replaced errors" = Table.ReplaceErrorValues(#"Removed columns 7", {
        {"u_project_name", ""}, {"saturday", ""}, {"project_number", ""}, {"u_prj_unit_resp", ""}, {"u_rate_role", ""}, {"u_timecard_source", ""}, {"sys_updated_on", ""}, {"user_name", ""}, {"u_dept_cost_center", ""}, {"sys_id", ""}, {"total", ""}, {"sys_updated_by", ""}, {"resource_plan.short_description", ""}, {"week_starts_on", ""}, {"wednesday", ""}, {"friday", ""}, {"state", ""}, {"resource_plan", ""}, {"project_sys_id", ""}, {"u_project_manager", ""}, {"monday", ""}, {"thursday", ""}, {"sunday", ""}, {"u_resource_manager", ""}, {"task", ""}, {"tuesday", ""}, {"task.short_description", ""}, {"category", ""}, {"user", ""}, {"user_sys_id", ""}, {"u_activity_cluster", ""}
    })
in
    #"Replaced errors";
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
shared time_card_b = let
    // Parameters
    startDate   = Date.FromText(QueryParameters{0}[Value]),
    endDate     = Date.FromText(QueryParameters{1}[Value]),
    PageSize    = Number.FromText(QueryParameters{2}[Value]),
    CompanyCodes= QueryParameters{3}[Value],
    Debtor      = QueryParameters{4}[Value],

    // Function to fetch one page
    GetPage = (Offset as number) =>
        let
            Response = Json.Document(
                Web.Contents(
                    "https://aztech.service-now.com/api/now/table/time_card",
                    [
                        Query = [
                            sysparm_display_value="true",
                            sysparm_exclude_reference_link="false",
                            sysparm_fields="u_project_name,top_task,task,u_activity_cluster,total,week_starts_on,monday,tuesday,wednesday,thursday,friday,saturday,sunday,category,state,u_dept_cost_center,u_prj_unit_resp,u_rep_project,u_timecard_source,u_rate_role,user.user_name,u_project_manager,u_resource_manager,resource_plan,user,sys_id,sys_updated_by,sys_updated_on,resource_plan.short_description,task.short_description",
sysparm_query = "week_starts_onBETWEEN" & Date.ToText(startDate, "yyyy-MM-dd") & "@" & Date.ToText(endDate, "yyyy-MM-dd") & "state!=Locked^total!=0^categorySTARTSWITHProject^top_task.ref_pm_project.u_accounting_master.u_master_approved!=Rejected^user.user_nameISNOTEMPTY^" & "user.u_debtor.sys_id=" & Debtor & "^top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeNOT IN" & CompanyCodes,
                            sysparm_limit = Number.ToText(PageSize),
                            sysparm_offset = Number.ToText(Offset)
                        ],
                        Headers=[Accept="application/json"],
                        Timeout=#duration(0,0,30,0)
                    ]
                )
            ),
            Result = try Response[result] otherwise {}
        in
            Result,

    // Keep calling pages until no rows returned
    Pages = List.Generate(
        () => [Offset = 0, Page = GetPage(0)],
        each List.Count([Page]) > 0,
        each [Offset = [Offset] + PageSize, Page = GetPage([Offset])],
        each [Page]
    ),

    // Flatten all pages
    AllRows = List.Combine(Pages),
    #"Converted to Table" = Table.FromList(AllRows, Splitter.SplitByNothing(), {"result"}),

    // === Your full transformation logic ===
    #"Expanded result" = Table.ExpandRecordColumn(#"Converted to Table", "result", {"u_project_name", "saturday", "top_task", "u_prj_unit_resp", "u_rate_role", "u_timecard_source", "sys_updated_on", "user.user_name", "u_dept_cost_center", "sys_id", "total", "sys_updated_by", "resource_plan.short_description", "week_starts_on", "wednesday", "friday", "state", "resource_plan", "u_rep_project", "u_project_manager", "monday", "thursday", "sunday", "u_resource_manager", "task", "tuesday", "task.short_description", "category", "user", "u_activity_cluster"}, {"u_project_name", "saturday", "top_task", "u_prj_unit_resp", "u_rate_role", "u_timecard_source", "sys_updated_on", "user.user_name", "u_dept_cost_center", "sys_id", "total", "sys_updated_by", "resource_plan.short_description", "week_starts_on", "wednesday", "friday", "state", "resource_plan", "u_rep_project", "u_project_manager", "monday", "thursday", "sunday", "u_resource_manager", "task", "tuesday", "task.short_description", "category", "user", "u_activity_cluster"}),

    #"Expanded user" = Table.ExpandRecordColumn(#"Expanded result", "user", {"display_value", "link"}, {"display_value", "link"}),
    #"Expanded u_prj_unit_resp" = Table.ExpandRecordColumn(#"Expanded user", "u_prj_unit_resp", {"display_value", "link"}, {"display_value.1", "link.1"}),
    #"Expanded top_task" = Table.ExpandRecordColumn(#"Expanded u_prj_unit_resp", "top_task", {"display_value", "link"}, {"display_value.2", "link.2"}),
    #"Expanded u_resource_manager" = Table.ExpandRecordColumn(#"Expanded top_task", "u_resource_manager", {"display_value", "link"}, {"display_value.3", "link.3"}),
    #"Expanded task" = Table.ExpandRecordColumn(#"Expanded u_resource_manager", "task", {"display_value", "link"}, {"display_value.4", "link.4"}),
    #"Expanded u_dept_cost_center" = Table.ExpandRecordColumn(#"Expanded task", "u_dept_cost_center", {"display_value", "link"}, {"display_value.5", "link.5"}),

    #"Renamed columns" = Table.RenameColumns(#"Expanded u_dept_cost_center", {{"display_value.2", "project_number"}}),
    #"Expanded u_rep_project" = Table.ExpandRecordColumn(#"Renamed columns", "u_rep_project", {"display_value", "link"}, {"display_value.2", "link.6"}),
    #"Removed columns" = Table.RemoveColumns(#"Expanded u_rep_project", {"link.2"}),
    #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"display_value.1", "u_prj_unit_resp"}}),
    #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 1", {"link.1", "link.5"}),
    #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"display_value.5", "u_dept_cost_center"}, {"user.user_name", "user_name"}}),
    #"Split column by delimiter" = Table.SplitColumn(Table.TransformColumnTypes(#"Renamed columns 2", {{"link.6", type text}}), "link.6", Splitter.SplitTextByEachDelimiter({"/"}, QuoteStyle.Csv, true), {"link.7", "link.8"}),
    #"Removed columns 2" = Table.RemoveColumns(#"Split column by delimiter", {"link.7"}),
    #"Renamed columns 3" = Table.RenameColumns(#"Removed columns 2", {{"link.8", "project_sys_id"}}),
    #"Expanded u_project_manager" = Table.ExpandRecordColumn(#"Renamed columns 3", "u_project_manager", {"display_value", "link"}, {"display_value.1", "link.1"}),
    #"Renamed columns 4" = Table.RenameColumns(#"Expanded u_project_manager", {{"display_value.1", "u_project_manager"}}),
    #"Removed columns 3" = Table.RemoveColumns(#"Renamed columns 4", {"link.1"}),
    #"Renamed columns 5" = Table.RenameColumns(#"Removed columns 3", {{"display_value.3", "u_resource_manager"}}),
    #"Removed columns 4" = Table.RemoveColumns(#"Renamed columns 5", {"link.3"}),
    #"Renamed columns 6" = Table.RenameColumns(#"Removed columns 4", {{"display_value.4", "task"}}),
    #"Removed columns 5" = Table.RemoveColumns(#"Renamed columns 6", {"link.4"}),
    #"Renamed columns 7" = Table.RenameColumns(#"Removed columns 5", {{"display_value", "user"}}),
    #"Split column by delimiter 1" = Table.SplitColumn(Table.TransformColumnTypes(#"Renamed columns 7", {{"link", type text}}), "link", Splitter.SplitTextByEachDelimiter({"/"}, QuoteStyle.Csv, true), {"link.1", "link.2"}),
    #"Removed columns 6" = Table.RemoveColumns(#"Split column by delimiter 1", {"link.1"}),
    #"Renamed columns 8" = Table.RenameColumns(#"Removed columns 6", {{"link.2", "user_sys_id"}}),
    #"Expanded resource_plan" = Table.ExpandRecordColumn(#"Renamed columns 8", "resource_plan", {"display_value", "link"}, {"display_value", "link"}),
    #"Renamed columns 9" = Table.RenameColumns(#"Expanded resource_plan", {{"display_value", "resource_plan"}}),
    #"Removed columns 7" = Table.RemoveColumns(#"Renamed columns 9", {"display_value.2", "link"}),

    #"Replaced errors" = Table.ReplaceErrorValues(#"Removed columns 7", {
        {"u_project_name", ""}, {"saturday", ""}, {"project_number", ""}, {"u_prj_unit_resp", ""}, {"u_rate_role", ""}, {"u_timecard_source", ""}, {"sys_updated_on", ""}, {"user_name", ""}, {"u_dept_cost_center", ""}, {"sys_id", ""}, {"total", ""}, {"sys_updated_by", ""}, {"resource_plan.short_description", ""}, {"week_starts_on", ""}, {"wednesday", ""}, {"friday", ""}, {"state", ""}, {"resource_plan", ""}, {"project_sys_id", ""}, {"u_project_manager", ""}, {"monday", ""}, {"thursday", ""}, {"sunday", ""}, {"u_resource_manager", ""}, {"task", ""}, {"tuesday", ""}, {"task.short_description", ""}, {"category", ""}, {"user", ""}, {"user_sys_id", ""}, {"u_activity_cluster", ""}
    })
in
    #"Replaced errors";
shared time_card_c = let
    // Parameters
    startDate   = Date.FromText(QueryParameters{0}[Value]),
    endDate     = Date.FromText(QueryParameters{1}[Value]),
    PageSize    = Number.FromText(QueryParameters{2}[Value]),
    CompanyCodes= QueryParameters{3}[Value],
    Debtor      = QueryParameters{4}[Value],

    // Function to fetch one page
    GetPage = (Offset as number) =>
        let
            Response = Json.Document(
                Web.Contents(
                    "https://aztech.service-now.com/api/now/table/time_card",
                    [
                        Query = [
                            sysparm_display_value="true",
                            sysparm_exclude_reference_link="false",
                            sysparm_fields="u_project_name,top_task,task,u_activity_cluster,total,week_starts_on,monday,tuesday,wednesday,thursday,friday,saturday,sunday,category,state,u_dept_cost_center,u_prj_unit_resp,u_rep_project,u_timecard_source,u_rate_role,user.user_name,u_project_manager,u_resource_manager,resource_plan,user,sys_id,sys_updated_by,sys_updated_on,resource_plan.short_description,task.short_description",
                            sysparm_query = "week_starts_onBETWEEN" & Date.ToText(startDate, "yyyy-MM-dd") & "@" & Date.ToText(endDate, "yyyy-MM-dd") & "state!=Locked^total!=0^categoryNOT LIKEProject^user.user_nameISNOTEMPTY^" & "user.u_debtor.sys_id=" & Debtor,
                            sysparm_limit = Number.ToText(PageSize),
                            sysparm_offset = Number.ToText(Offset)
                        ],
                        Headers=[Accept="application/json"],
                        Timeout=#duration(0,0,30,0)
                    ]
                )
            ),
            Result = try Response[result] otherwise {}
        in
            Result,

    // Keep calling pages until no rows returned
    Pages = List.Generate(
        () => [Offset = 0, Page = GetPage(0)],
        each List.Count([Page]) > 0,
        each [Offset = [Offset] + PageSize, Page = GetPage([Offset])],
        each [Page]
    ),

    // Flatten all pages
    AllRows = List.Combine(Pages),
    #"Converted to Table" = Table.FromList(AllRows, Splitter.SplitByNothing(), {"result"}),

    // === Your full transformation logic ===
    #"Expanded result" = Table.ExpandRecordColumn(#"Converted to Table", "result", {"u_project_name", "saturday", "top_task", "u_prj_unit_resp", "u_rate_role", "u_timecard_source", "sys_updated_on", "user.user_name", "u_dept_cost_center", "sys_id", "total", "sys_updated_by", "resource_plan.short_description", "week_starts_on", "wednesday", "friday", "state", "resource_plan", "u_rep_project", "u_project_manager", "monday", "thursday", "sunday", "u_resource_manager", "task", "tuesday", "task.short_description", "category", "user", "u_activity_cluster"}, {"u_project_name", "saturday", "top_task", "u_prj_unit_resp", "u_rate_role", "u_timecard_source", "sys_updated_on", "user.user_name", "u_dept_cost_center", "sys_id", "total", "sys_updated_by", "resource_plan.short_description", "week_starts_on", "wednesday", "friday", "state", "resource_plan", "u_rep_project", "u_project_manager", "monday", "thursday", "sunday", "u_resource_manager", "task", "tuesday", "task.short_description", "category", "user", "u_activity_cluster"}),

    #"Expanded user" = Table.ExpandRecordColumn(#"Expanded result", "user", {"display_value", "link"}, {"display_value", "link"}),
    #"Expanded u_prj_unit_resp" = Table.ExpandRecordColumn(#"Expanded user", "u_prj_unit_resp", {"display_value", "link"}, {"display_value.1", "link.1"}),
    #"Expanded top_task" = Table.ExpandRecordColumn(#"Expanded u_prj_unit_resp", "top_task", {"display_value", "link"}, {"display_value.2", "link.2"}),
    #"Expanded u_resource_manager" = Table.ExpandRecordColumn(#"Expanded top_task", "u_resource_manager", {"display_value", "link"}, {"display_value.3", "link.3"}),
    #"Expanded task" = Table.ExpandRecordColumn(#"Expanded u_resource_manager", "task", {"display_value", "link"}, {"display_value.4", "link.4"}),
    #"Expanded u_dept_cost_center" = Table.ExpandRecordColumn(#"Expanded task", "u_dept_cost_center", {"display_value", "link"}, {"display_value.5", "link.5"}),

    #"Renamed columns" = Table.RenameColumns(#"Expanded u_dept_cost_center", {{"display_value.2", "project_number"}}),
    #"Expanded u_rep_project" = Table.ExpandRecordColumn(#"Renamed columns", "u_rep_project", {"display_value", "link"}, {"display_value.2", "link.6"}),
    #"Removed columns" = Table.RemoveColumns(#"Expanded u_rep_project", {"link.2"}),
    #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"display_value.1", "u_prj_unit_resp"}}),
    #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 1", {"link.1", "link.5"}),
    #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"display_value.5", "u_dept_cost_center"}, {"user.user_name", "user_name"}}),
    #"Split column by delimiter" = Table.SplitColumn(Table.TransformColumnTypes(#"Renamed columns 2", {{"link.6", type text}}), "link.6", Splitter.SplitTextByEachDelimiter({"/"}, QuoteStyle.Csv, true), {"link.7", "link.8"}),
    #"Removed columns 2" = Table.RemoveColumns(#"Split column by delimiter", {"link.7"}),
    #"Renamed columns 3" = Table.RenameColumns(#"Removed columns 2", {{"link.8", "project_sys_id"}}),
    #"Expanded u_project_manager" = Table.ExpandRecordColumn(#"Renamed columns 3", "u_project_manager", {"display_value", "link"}, {"display_value.1", "link.1"}),
    #"Renamed columns 4" = Table.RenameColumns(#"Expanded u_project_manager", {{"display_value.1", "u_project_manager"}}),
    #"Removed columns 3" = Table.RemoveColumns(#"Renamed columns 4", {"link.1"}),
    #"Renamed columns 5" = Table.RenameColumns(#"Removed columns 3", {{"display_value.3", "u_resource_manager"}}),
    #"Removed columns 4" = Table.RemoveColumns(#"Renamed columns 5", {"link.3"}),
    #"Renamed columns 6" = Table.RenameColumns(#"Removed columns 4", {{"display_value.4", "task"}}),
    #"Removed columns 5" = Table.RemoveColumns(#"Renamed columns 6", {"link.4"}),
    #"Renamed columns 7" = Table.RenameColumns(#"Removed columns 5", {{"display_value", "user"}}),
    #"Split column by delimiter 1" = Table.SplitColumn(Table.TransformColumnTypes(#"Renamed columns 7", {{"link", type text}}), "link", Splitter.SplitTextByEachDelimiter({"/"}, QuoteStyle.Csv, true), {"link.1", "link.2"}),
    #"Removed columns 6" = Table.RemoveColumns(#"Split column by delimiter 1", {"link.1"}),
    #"Renamed columns 8" = Table.RenameColumns(#"Removed columns 6", {{"link.2", "user_sys_id"}}),
    #"Expanded resource_plan" = Table.ExpandRecordColumn(#"Renamed columns 8", "resource_plan", {"display_value", "link"}, {"display_value", "link"}),
    #"Renamed columns 9" = Table.RenameColumns(#"Expanded resource_plan", {{"display_value", "resource_plan"}}),
    #"Removed columns 7" = Table.RemoveColumns(#"Renamed columns 9", {"display_value.2", "link"}),

    #"Replaced errors" = Table.ReplaceErrorValues(#"Removed columns 7", {
        {"u_project_name", ""}, {"saturday", ""}, {"project_number", ""}, {"u_prj_unit_resp", ""}, {"u_rate_role", ""}, {"u_timecard_source", ""}, {"sys_updated_on", ""}, {"user_name", ""}, {"u_dept_cost_center", ""}, {"sys_id", ""}, {"total", ""}, {"sys_updated_by", ""}, {"resource_plan.short_description", ""}, {"week_starts_on", ""}, {"wednesday", ""}, {"friday", ""}, {"state", ""}, {"resource_plan", ""}, {"project_sys_id", ""}, {"u_project_manager", ""}, {"monday", ""}, {"thursday", ""}, {"sunday", ""}, {"u_resource_manager", ""}, {"task", ""}, {"tuesday", ""}, {"task.short_description", ""}, {"category", ""}, {"user", ""}, {"user_sys_id", ""}, {"u_activity_cluster", ""}
    })
in
    #"Replaced errors";
[BindToDefaultDestination = true]
shared time_card = let
  Source = Table.Combine({time_card_a, time_card_b, time_card_c}),
  #"Changed column type" = Table.TransformColumnTypes(Source, {{"u_project_name", type text}, {"saturday", type text}, {"project_number", type text}, {"u_prj_unit_resp", type text}, {"u_rate_role", type text}, {"u_timecard_source", type text}, {"sys_updated_on", type text}, {"user_name", type text}, {"u_dept_cost_center", type text}, {"sys_id", type text}, {"total", type text}, {"sys_updated_by", type text}, {"resource_plan.short_description", type text}, {"week_starts_on", type text}, {"wednesday", type text}, {"friday", type text}, {"state", type text}, {"resource_plan", type text}, {"project_sys_id", type text}, {"u_project_manager", type text}, {"monday", type text}, {"thursday", type text}, {"sunday", type text}, {"u_resource_manager", type text}, {"task", type text}, {"tuesday", type text}, {"task.short_description", type text}, {"category", type text}, {"user", type text}, {"user_sys_id", type text}, {"u_activity_cluster", type text}})
in
  #"Changed column type";
