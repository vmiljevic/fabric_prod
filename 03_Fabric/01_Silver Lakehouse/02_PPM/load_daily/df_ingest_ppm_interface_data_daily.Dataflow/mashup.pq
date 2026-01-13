section Section1;
shared CompanyCode = "AT45" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared NumberOfRecords = "9999" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared DateFrom = "2025-02-01" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared DateTo = "2025-02-01" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared Debtor = "9d2a756969c65200e4854a54f2f9271a" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared time_card_interface_set_a = let
    //
    // PARAMETERS
    //
    startDate = Date.FromText(DateFrom),      
    nextDate  = Date.AddDays(Date.FromText(DateTo), 1), 

    RecordLimit  = Number.FromText(NumberOfRecords),
    CompanyCodes = CompanyCode,

    // Safe UTC window: [yesterday 00:00 → today 00:00)
    startDateText = Date.ToText(startDate, "yyyy-MM-dd") & " 00:00:00",
    nextDateText  = Date.ToText(nextDate,  "yyyy-MM-dd") & " 00:00:00",


    //
    // SNOW FILTER – sys_updated_on SAFE LOGIC
    //
    queryFilter =
        "sys_updated_on>=" & startDateText &
        "^sys_updated_on<" & nextDateText &
        "^tc.state!=Locked" &
        "^tc.total!=0" &
        "^tc.categorySTARTSWITHProject" &
        "^tc.user.user_nameISNOTEMPTY" &
        "^rep_hours!=0" &
        "^tc.top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeIN" & CompanyCodes,


    //
    // REQUEST
    //
    Source =
        Json.Document(
            Web.Contents(
                "https://aztech.service-now.com",
                [
                    RelativePath = "api/now/table/x_ppm_time_card_interface",
                    Query = [
                        sysparm_display_value = "true",
                        sysparm_exclude_reference_link = "false",
                        sysparm_fields =
                            "tc,comments,postingdate,state,tc.top_task.sys_id,sys_updated_on",
                        sysparm_query = queryFilter,
                        sysparm_limit = Number.ToText(RecordLimit)
                    ],
                    Headers = [Accept = "application/json"]
                ]
            )
        ),


    //
    // TRANSFORM
    //
    #"Converted to Table" = Table.FromRecords({Source}),
    #"Expanded result"    = Table.ExpandListColumn(#"Converted to Table", "result"),
    #"Expanded record"    = Table.ExpandRecordColumn(
                                #"Expanded result",
                                "result",
                                {"tc", "comments", "postingdate", "state", "tc.top_task.sys_id", "sys_updated_on"},
                                {"tc", "comments", "postingdate", "state", "tc.top_task.sys_id", "sys_updated_on"}
                           ),

    #"Expanded tc" = Table.ExpandRecordColumn(
                        #"Expanded record",
                        "tc",
                        {"display_value", "link"},
                        {"tc.display_value", "tc.link"}
                    ),

    #"Extracted SysId" =
        Table.TransformColumns(
            #"Expanded tc",
            {{"tc.link", each Text.AfterDelimiter(_, "/", 6), type text}}
        ),

    #"Renamed Columns" =
        Table.RenameColumns(
            #"Extracted SysId",
            {
                {"tc.link", "tc_sys_id"},
                {"tc.display_value", "week_starts_on"},
                {"postingdate", "posting_date"}
            }
        ),

    #"Added ExportFor" =
        Table.AddColumn(#"Renamed Columns", "ExportFor", each CompanyCode),

    #"Transform Types" =
        Table.TransformColumnTypes(
            #"Added ExportFor",
            {
                {"week_starts_on", type text},
                {"comments", type text},
                {"posting_date", type text},
                {"state", type text},
                {"ExportFor", type text},
                {"tc.top_task.sys_id", type text},
                {"sys_updated_on", type text}
            }
        ),

    #"Replace Errors" =
        Table.ReplaceErrorValues(
            #"Transform Types",
            {
                {"week_starts_on", null},
                {"comments", null},
                {"posting_date", null},
                {"state", null},
                {"ExportFor", null},
                {"tc.top_task.sys_id", null}
            }
        ),

    #"Rename project sys" =
        Table.RenameColumns(#"Replace Errors",
            {{"tc.top_task.sys_id", "project_sys_id"}})

in
    #"Rename project sys";
shared time_card_interface_set_b = let
    //
    // PARAMETERS
    //
    startDate = Date.FromText(DateFrom),      
    nextDate  = Date.AddDays(Date.FromText(DateTo), 1), 

    RecordLimit  = Number.FromText(NumberOfRecords),
    CompanyCodes = CompanyCode,
    Debtor       = Debtor,

    // Safe window: [yesterday 00:00 → today 00:00)
    startDateText = Date.ToText(startDate, "yyyy-MM-dd") & " 00:00:00",
    nextDateText  = Date.ToText(nextDate,  "yyyy-MM-dd") & " 00:00:00",


    //
    // SNOW FILTER – SAFE sys_updated_on LOGIC
    //
    queryFilter =
        "sys_updated_on>=" & startDateText &
        "^sys_updated_on<" & nextDateText &
        "^tc.state!=Locked" &
        "^tc.total!=0" &
        "^tc.categorySTARTSWITHProject" &
        "^tc.user.user_nameISNOTEMPTY" &
        "^tc.user.u_debtor.sys_id=" & Debtor &
        "^rep_hours!=0" &
        "^tc.top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeNOT IN" & CompanyCodes,


    //
    // REQUEST
    //
    Source =
        Json.Document(
            Web.Contents(
                "https://aztech.service-now.com",
                [
                    RelativePath = "api/now/table/x_ppm_time_card_interface",
                    Query = [
                        sysparm_display_value = "true",
                        sysparm_exclude_reference_link = "false",
                        sysparm_query = queryFilter,
                        sysparm_fields =
                            "tc,comments,postingdate,state,tc.top_task.sys_id,sys_updated_on",
                        sysparm_limit = Number.ToText(RecordLimit)
                    ],
                    Headers = [Accept = "application/json"]
                ]
            )
        ),


    //
    // TRANSFORMATIONS
    //
    #"Converted to Table" = Table.FromRecords({Source}),

    #"Expanded result" =
        Table.ExpandListColumn(#"Converted to Table", "result"),

    #"Expanded record" =
        Table.ExpandRecordColumn(
            #"Expanded result",
            "result",
            {"tc", "comments", "postingdate", "state", "tc.top_task.sys_id", "sys_updated_on"},
            {"tc", "comments", "postingdate", "state", "tc.top_task.sys_id", "sys_updated_on"}
        ),

    #"Expanded tc" =
        Table.ExpandRecordColumn(
            #"Expanded record",
            "tc",
            {"display_value", "link"},
            {"tc.display_value", "tc.link"}
        ),

    #"Extracted SysId" =
        Table.TransformColumns(
            #"Expanded tc",
            {{"tc.link", each Text.AfterDelimiter(_, "/", 6), type text}}
        ),

    #"Renamed Columns" =
        Table.RenameColumns(
            #"Extracted SysId",
            {
                {"tc.link", "tc_sys_id"},
                {"tc.display_value", "week_starts_on"},
                {"postingdate", "posting_date"}
            }
        ),

    #"Added ExportFor" =
        Table.AddColumn(#"Renamed Columns", "ExportFor", each CompanyCode),

    #"Transform Types" =
        Table.TransformColumnTypes(
            #"Added ExportFor",
            {
                {"week_starts_on", type text},
                {"comments", type text},
                {"posting_date", type text},
                {"state", type text},
                {"ExportFor", type text},
                {"tc.top_task.sys_id", type text},
                {"sys_updated_on", type text}
            }
        ),

    #"Replace Errors" =
        Table.ReplaceErrorValues(
            #"Transform Types",
            {
                {"week_starts_on", null},
                {"comments", null},
                {"posting_date", null},
                {"state", null},
                {"ExportFor", null},
                {"tc.top_task.sys_id", null}
            }
        ),

    #"Renamed project sys" =
        Table.RenameColumns(#"Replace Errors",
            {{"tc.top_task.sys_id", "project_sys_id"}})

in
    #"Renamed project sys";
shared time_card_interface_externals = let
    //
    // PARAMETERS
    //
    startDate = Date.FromText(DateFrom),      
    nextDate  = Date.AddDays(Date.FromText(DateTo), 1), 

    RecordLimit  = Number.FromText(NumberOfRecords),
    CompanyCodes = CompanyCode,
    Debtor       = Debtor,

    // Safe UTC window: [yesterday 00:00 → today 00:00)
    startDateText = Date.ToText(startDate, "yyyy-MM-dd") & " 00:00:00",
    nextDateText  = Date.ToText(nextDate,  "yyyy-MM-dd") & " 00:00:00",


    //
    // SNOW FILTER – SAFE sys_updated_on LOGIC
    //
    queryFilter =
        "sys_updated_on>=" & startDateText &
        "^sys_updated_on<" & nextDateText &
        "^timecard.user.u_debtor.sys_id=" & Debtor &
        "^hours!=0",


    //
    // REQUEST
    //
    Source =
        Json.Document(
            Web.Contents(
                "https://aztech.service-now.com",
                [
                    RelativePath = "api/now/table/x_ppm_ses_data",
                    Query = [
                        sysparm_display_value = "true",
                        sysparm_exclude_reference_link = "false",
                        sysparm_query = queryFilter,
                        sysparm_fields =
                            "timecard,comments,posting_date,state,documentdate,raterole,rate_per_hour,timecard.top_task.sys_id,sys_updated_on,ponumber,currency",
                        sysparm_limit = Number.ToText(RecordLimit)
                    ],
                    Headers = [Accept = "application/json"]
                ]
            )
        ),


    //
    // TRANSFORMATIONS
    //
    #"Converted to Table" = Table.FromRecords({Source}),
    #"Expanded result" = Table.ExpandListColumn(#"Converted to Table", "result"),

    #"Expanded record" =
        Table.ExpandRecordColumn(
            #"Expanded result",
            "result",
            {
                "timecard",
                "comments",
                "posting_date",
                "state",
                "documentdate",
                "raterole",
                "rate_per_hour",
                "timecard.top_task.sys_id",
                "sys_updated_on",
                "ponumber",
                "currency"
                
            },
            {
                "timecard",
                "comments",
                "posting_date",
                "state",
                "documentdate",
                "raterole",
                "rate_per_hour",
                "timecard.top_task.sys_id",
                "sys_updated_on",
                "ponumber",
                "currency"
            }
        ),

    #"Expanded timecard" =
        Table.ExpandRecordColumn(
            #"Expanded record",
            "timecard",
            {"display_value", "link"},
            {"display_value", "link"}
        ),

    #"Extracted SysId" =
        Table.TransformColumns(
            #"Expanded timecard",
            {{"link", each Text.AfterDelimiter(_, "/", 6), type text}}
        ),

    #"Renamed Columns" =
        Table.RenameColumns(
            #"Extracted SysId",
            {
                {"display_value", "week_starts_on"},
                {"link", "tc_sys_id"}
            }
        ),

    #"Removed Columns" =
        Table.RemoveColumns(#"Renamed Columns", {"documentdate"}),

    #"Added ExportFor" =
        Table.AddColumn(#"Removed Columns", "ExportFor", each CompanyCode),

    #"Renamed project sys" =
        Table.RenameColumns(#"Added ExportFor",
            {{"timecard.top_task.sys_id", "project_sys_id"}}),

    #"Transform Types" =
        Table.TransformColumnTypes(#"Renamed project sys", {{"week_starts_on", type text}, {"comments", type text}, {"posting_date", type text}, {"state", type text}, {"raterole", type text}, {"rate_per_hour", type text}, {"ExportFor", type text}, {"project_sys_id", type text}, {"sys_updated_on", type text}, {"ponumber", type text}, {"currency", type text}}),

    #"Replace Errors" =
        Table.ReplaceErrorValues(
            #"Transform Types",
            {
                {"week_starts_on", null},
                {"comments", null},
                {"posting_date", null},
                {"state", null},
                {"raterole", null},
                {"rate_per_hour", null},
                {"ExportFor", null},
                {"project_sys_id", null}
            }
        )

in
    #"Replace Errors";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "fact_ppm_time_card_interface_daily_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "week_starts_on", DestinationColumnName = "week_starts_on"], [SourceColumnName = "tc_sys_id", DestinationColumnName = "tc_sys_id"], [SourceColumnName = "comments", DestinationColumnName = "comments"], [SourceColumnName = "posting_date", DestinationColumnName = "posting_date"], [SourceColumnName = "state", DestinationColumnName = "state"], [SourceColumnName = "raterole", DestinationColumnName = "raterole"], [SourceColumnName = "rate_per_hour", DestinationColumnName = "rate_per_hour"], [SourceColumnName = "ExportFor", DestinationColumnName = "ExportFor"], [SourceColumnName = "sys_updated_on", DestinationColumnName = "sys_updated_on"], [SourceColumnName = "ponumber", DestinationColumnName = "ponumber"], [SourceColumnName = "currency", DestinationColumnName = "currency"], [SourceColumnName = "project_sys_id", DestinationColumnName = "project_sys_id"]}], DynamicSchema = false, UpdateMethod = [Kind = "Append"], TypeSettings = [Kind = "Table"]]]}]
shared fact_ppm_time_card_interface_daily = let
  Source = Table.Combine({time_card_interface_externals, time_card_interface_set_a, time_card_interface_set_b}),
  #"Changed column type" = Table.TransformColumnTypes(Source, {{"sys_updated_on", type text}}),
  #"Filtered rows" = Table.SelectRows(#"Changed column type", each [tc_sys_id] <> "" and [tc_sys_id] <> null)
in
  #"Filtered rows";
shared fact_ppm_time_card_interface_daily_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = "fact_ppm_time_card_interface_daily", ItemKind = "Table"]}[Data]
in
  TableNavigation;
