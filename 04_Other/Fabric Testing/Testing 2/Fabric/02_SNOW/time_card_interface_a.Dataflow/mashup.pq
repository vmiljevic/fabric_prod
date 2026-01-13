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
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "time_card_interface_a_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "sys_id", DestinationColumnName = "sys_id"], [SourceColumnName = "week_starts_on", DestinationColumnName = "week_starts_on"], [SourceColumnName = "tc_sys_id", DestinationColumnName = "tc_sys_id"], [SourceColumnName = "comments", DestinationColumnName = "comments"], [SourceColumnName = "postingdate", DestinationColumnName = "postingdate"], [SourceColumnName = "state", DestinationColumnName = "state"], [SourceColumnName = "sys_updated_on", DestinationColumnName = "sys_updated_on"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared time_card_interface_set_a = let
    // Parameters
    startDate = DateWeek,
    endDate = DateWeek,
    RecordLimit = Number.FromText(QueryParameters{2}[Value]),
    CompanyCodes = QueryParameters{3}[Value],
    Debtor = QueryParameters{4}[Value],

    // Ensure ISO-8601 text for ServiceNow
    startDateText = if Value.Is(startDate, type date) then Date.ToText(startDate, "yyyy-MM-dd") else Text.From(startDate),
    endDateText   = if Value.Is(endDate,   type date) then Date.ToText(endDate,   "yyyy-MM-dd") else Text.From(endDate),

    // === Single API Call (no paging) ===
    Source = try Json.Document(
                Web.Contents(
                    "https://aztech.service-now.com",
                    [
                        RelativePath = "api/now/table/x_ppm_time_card_interface",
                        Query = [
                            sysparm_display_value = "true",
                            sysparm_exclude_reference_link = "false",
                            sysparm_fields = "sys_id,tc,comments,postingdate,state,sys_updated_on",
                            sysparm_limit  = Text.From(RecordLimit),
                            sysparm_query  = "tc.week_starts_onBETWEEN" & startDateText & "@" & startDateText &
                                             "^tc.state!=Locked^tc.total!=0^tc.categorySTARTSWITHProject" &
                                             "^tc.user.user_nameISNOTEMPTY^rep_hours!=0" &
                                             "^tc.top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeIN" & CompanyCodes &
                                             "^ORDERBYsys_created_on"
                        ]
                    ]
                )
              ) otherwise null,

    // Handle missing/empty result safely
    Result = if Source <> null and Record.HasFields(Source, "result") 
             then Source[result] 
             else {},

    // Convert to table (will be empty if no records)
    #"Converted to Table" = Table.FromRecords(Result, 
        {"sys_id","tc","comments","postingdate","state","sys_updated_on"}),

    // Expand "tc" only if present
    #"Expanded tc" = if Table.HasColumns(#"Converted to Table", "tc") 
                     then Table.ExpandRecordColumn(#"Converted to Table", "tc", {"display_value", "link"}, {"display_value", "link"}) 
                     else #"Converted to Table",
  #"Split column by delimiter" = Table.SplitColumn(Table.TransformColumnTypes(#"Expanded tc", {{"link", type text}}), "link", Splitter.SplitTextByEachDelimiter({"/"}, QuoteStyle.Csv, true), {"link.1", "link.2"}),
  #"Renamed columns" = Table.RenameColumns(#"Split column by delimiter", {{"link.2", "tc_sys_id"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns", {"link.1"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"display_value", "week_starts_on"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns 1", {{"sys_id", type text}, {"week_starts_on", type text}, {"tc_sys_id", type text}, {"comments", type text}, {"postingdate", type text}, {"state", type text}, {"sys_updated_on", type text}})
in
    #"Changed column type";
[BindToDefaultDestination = true]
shared DateWeek = "2025-09-07" meta [IsParameterQuery = true, IsParameterQueryRequired = true, Type = type text];
shared time_card_interface_a_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data],
  TableNavigation = Navigation_2{[Id = "time_card_interface_all", ItemKind = "Table"]}[Data]
in
  TableNavigation;
