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
[BindToDefaultDestination = true]
shared time_card_interface_set_b = let
    // Parameters
    startDate = QueryParameters{0}[Value],
    endDate = QueryParameters{1}[Value],
    RecordLimit = Number.FromText(QueryParameters{2}[Value]),
    CompanyCodes = QueryParameters{3}[Value],
    Debtor = QueryParameters{4}[Value],
    PageSize     = 1000,

    // Function to fetch one page
    GetPage = (Offset as number) =>
        let
            Source = Json.Document(
                Web.Contents(
                    "https://aztech.service-now.com",[
                    RelativePath = "api/now/table/x_ppm_time_card_interface",
                    
                        Query = [
                            sysparm_display_value = "true",
                            sysparm_exclude_reference_link = "false",
                            sysparm_fields = "sys_id,tc,comments,postingdate,state,sys_updated_on",
                            sysparm_limit  = Text.From(PageSize),
                            sysparm_offset = Text.From(Offset),
                            sysparm_query = "tc.week_starts_onBETWEEN" & startDate & "@" & endDate & 
                            "tc.state!=Locked^
                            tc.total!=0^
                            tc.categorySTARTSWITHProject^
                            tc.user.user_nameISNOTEMPTY^
                            tc.user.u_debtor.sys_id=" & Debtor & "^" &
                            "rep_hours!=0^
                            tc.top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeNOT IN" & CompanyCodes & "^ORDERBYsys_created_on"
                            
                        ]
                    ]
                )
            ),
            Result = try Source[result] otherwise {}
        in
            Result,

    // Loop until no rows are returned
    Pages = List.Generate(
        () => [Offset = 0, Data = GetPage(0)],
        each List.Count([Data]) > 0,
        each [Offset = [Offset] + PageSize, Data = GetPage([Offset])],
        each [Data]
    ),

    Combined = List.Combine(Pages),
    #"Converted to Table" = Table.FromRecords(Combined),
  #"Expanded tc" = Table.ExpandRecordColumn(#"Converted to Table", "tc", {"display_value", "link"}, {"display_value", "link"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded tc", {{"display_value", "week_starts_on"}}),
  #"Split column by delimiter" = Table.SplitColumn(Table.TransformColumnTypes(#"Renamed columns", {{"link", type text}}), "link", Splitter.SplitTextByEachDelimiter({"/"}, QuoteStyle.Csv, true), {"link.1", "link.2"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Split column by delimiter", {{"link.2", "tc_sys_id"}}),
  #"Removed columns" = Table.RemoveColumns(#"Renamed columns 1", {"link.1"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns", {{"sys_id", type text}, {"comments", type text}, {"postingdate", type text}, {"state", type text}, {"sys_updated_on", type text}, {"week_starts_on", type text}, {"tc_sys_id", type text}})

    
in
    #"Changed column type";
