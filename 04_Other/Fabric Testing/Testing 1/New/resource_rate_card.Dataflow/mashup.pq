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
shared resource_rate_card = let
    // Parameters
    startDate = Date.FromText(QueryParameters{0}[Value]),
    endDate = Date.FromText(QueryParameters{1}[Value]),
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
                    RelativePath = "api/now/table/x_ppm_resource_rate_card",
                    
                        Query = [
                            sysparm_display_value = "true",
                            sysparm_exclude_reference_link = "false",
                            sysparm_fields = "user,co_rate_role,active,sys_created_on",
                            sysparm_query = "ORDERBYsys_created_on",
                            sysparm_limit  = Text.From(PageSize),
                            sysparm_offset = Text.From(Offset)
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
  #"Filtered rows" = Table.SelectRows(#"Converted to Table", each [co_rate_role] <> "" and [user] <> ""),
  #"Expanded co_rate_role" = Table.ExpandRecordColumn(#"Filtered rows", "co_rate_role", {"display_value", "link"}, {"display_value", "link"}),
  #"Expanded user" = Table.ExpandRecordColumn(#"Expanded co_rate_role", "user", {"display_value", "link"}, {"display_value.1", "link.1"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded user", {{"display_value", "rate_role"}}),
  #"Split column by delimiter" = Table.SplitColumn(Table.TransformColumnTypes(#"Renamed columns", {{"link", type text}}), "link", Splitter.SplitTextByEachDelimiter({"/"}, QuoteStyle.Csv, true), {"link.2", "link.3"}),
  #"Removed columns" = Table.RemoveColumns(#"Split column by delimiter", {"link.2"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Removed columns", {{"link.3", "rate_role_sys_id"}, {"display_value.1", "user"}}),
  #"Split column by delimiter 1" = Table.SplitColumn(Table.TransformColumnTypes(#"Renamed columns 1", {{"link.1", type text}}), "link.1", Splitter.SplitTextByEachDelimiter({"/"}, QuoteStyle.Csv, true), {"link.2", "link.3"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Split column by delimiter 1", {{"link.3", "user_sys_id"}}),
  #"Removed columns 1" = Table.RemoveColumns(#"Renamed columns 2", {"link.2"}),
  #"Changed column type" = Table.TransformColumnTypes(#"Removed columns 1", {{"sys_created_on", type text}, {"user_sys_id", type text}, {"rate_role", type text}, {"rate_role_sys_id", type text}, {"active", type text}, {"user", type text}})

    
in
    #"Changed column type";
