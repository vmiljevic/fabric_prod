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
shared time_card_interface_externals = let
    // Parameters
    startDate = QueryParameters{0}[Value],
    endDate = QueryParameters{1}[Value],
    RecordLimit = Number.FromText(QueryParameters{2}[Value]),
    CompanyCodes = QueryParameters{3}[Value],
    Debtor = QueryParameters{4}[Value],
    Year = Text.Start(QueryParameters{1}[Value], 4),
    PageSize     = 1000,

    // Function to fetch one page
    GetPage = (Offset as number) =>
        let
            Source = Json.Document(
                Web.Contents(
                    "https://aztech.service-now.com",[
                    RelativePath = "api/now/table/x_ppm_ses_data",
                    
                        Query = [
                            sysparm_display_value = "true",
                            sysparm_exclude_reference_link = "false",
                            sysparm_fields = "sys_id,timecard,comments,posting_date,state,documentdate,raterole,rate_per_hour,sys_updated_on",
                            sysparm_limit  = Text.From(PageSize),
                            sysparm_offset = Text.From(Offset),
                            sysparm_query = "timecard.user.u_debtor.sys_id=" & Debtor & "^" & "hours!=0^documentdateLIKE" & Year & "^ORDERBYsys_created_on"
                            
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
  #"Expanded timecard" = Table.ExpandRecordColumn(#"Converted to Table", "timecard", {"display_value", "link"}, {"display_value", "link"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded timecard", {{"display_value", "weeks_starts_on"}}),
  #"Split column by delimiter" = Table.SplitColumn(Table.TransformColumnTypes(#"Renamed columns", {{"link", type text}}), "link", Splitter.SplitTextByEachDelimiter({"/"}, QuoteStyle.Csv, true), {"link.1", "link.2"}),
  #"Removed columns" = Table.RemoveColumns(#"Split column by delimiter", {"link.1"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"link.2", "tc_sys_id"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns 1", {{"rate_per_hour", type text}, {"sys_id", type text}, {"comments", type text}, {"weeks_starts_on", type text}, {"tc_sys_id", type text}, {"state", type text}, {"sys_updated_on", type text}, {"posting_date", type text}, {"documentdate", type text}, {"raterole", type text}})

    
in
    #"Changed column type";
