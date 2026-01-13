[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared CompanyCode = "CZ45" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type any, DefaultValue = ""];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "u_ppm_accounting_element_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared u_ppm_accounting_element = let
  RecordLimit = Number.FromText(NumberOfRecords),
  CompanyCodes = CompanyCode,
  Source = Json.Document(
        Web.Contents(
                 "https://aztech.service-now.com",
        [
            RelativePath = "api/now/table/u_ppm_accounting_element",
            Query = [
                    sysparm_display_value="true",
                    sysparm_exclude_reference_link="false",
                    sysparm_query="u_wbs_element_idSTARTSWITH" & CompanyCodes,
                    sysparm_limit=Number.ToText(RecordLimit)
                ],
                Headers=[Accept="application/json"]
            ]
        )
    ),
  #"Converted to Table" = Table.FromRecords({Source}),
  #"Expanded result1" = Table.ExpandListColumn(#"Converted to Table", "result"),
  #"Expanded result3" = Table.ExpandRecordColumn(#"Expanded result1", "result", {"u_accounting_master", "u_wbs_element_id", "u_element_approved", "u_expense_type", "u_name","sys_updated_on"}, {"u_accounting_master", "u_wbs_element_id", "u_element_approved", "u_expense_type", "u_name","sys_updated_on"}),
  #"Expanded u_accounting_master" = Table.ExpandRecordColumn(#"Expanded result3", "u_accounting_master", {"display_value", "link"}, {"display_value", "link"}),
  #"Renamed Columns" = Table.RenameColumns(#"Expanded u_accounting_master", {{"display_value", "accounting_master"}}),
  #"Removed Columns" = Table.RemoveColumns(#"Renamed Columns", {"link"}),
  #"Added Custom" = Table.AddColumn(#"Removed Columns", "ExportFor", each CompanyCode),
  #"Transform columns" = Table.TransformColumnTypes(#"Added Custom", {{"accounting_master", type text}, {"u_wbs_element_id", type text}, {"u_element_approved", type text}, {"u_expense_type", type text}, {"u_name", type text}, {"ExportFor", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"accounting_master", null}, {"u_wbs_element_id", null}, {"u_element_approved", null}, {"u_expense_type", null}, {"u_name", null}, {"ExportFor", null}})
in
  #"Replace errors";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "pm_projects_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared pm_projects = let
  // Step 1: Get list of project IDs from interface
  startDate = DateFrom,
  endDate = DateTo,
  RecordLimit = Number.FromText(NumberOfRecords),
  CompanyCodes = CompanyCode,
  RawSource = Json.Document(
         Web.Contents(
                 "https://aztech.service-now.com",
        [
            RelativePath = "api/now/table/x_ppm_project_wbs_interface",
            Query = [
                    sysparm_display_value = "true",
                    sysparm_exclude_reference_link = "false",
                    sysparm_query = "posidLIKE" & CompanyCodes & "^ORpbukrLIKE" & CompanyCodes &  "^project_idISNOTEMPTY",
                    sysparm_limit = Number.ToText(RecordLimit)
                ],
                Headers = [Accept = "application/json"]
            ]
        )
    ),
  InterfaceTable = Table.FromRecords({RawSource}),
  ExpandedList = Table.ExpandListColumn(InterfaceTable, "result"),
  ExpandedResult = Table.ExpandRecordColumn(ExpandedList, "result", {"project_id"}, {"project_id"}),
  #"Filtered Rows" = Table.SelectRows(ExpandedResult, each [project_id] <> ""),
  ExpandedProjectId = Table.ExpandRecordColumn(#"Filtered Rows", "project_id", {"display_value", "link"}, {"project_display", "project_link"}),
  CleanedProjectIds = Table.TransformColumns(ExpandedProjectId, {{"project_link", each Text.AfterDelimiter(_, "/", 6), type text}}),
  UniqueProjectIds = List.Distinct(CleanedProjectIds[project_link]),
  // Step 2: Fetch full project info from pm_project using project sys_ids in batches
  fetchProjectBatch = (batch as list) =>
        let
            sysIdString = Text.Combine(batch, ","),
            queryFilter = "sys_idIN" & sysIdString,
            response = Json.Document(
                Web.Contents(
                    "https://aztech.service-now.com",
                    [
                        RelativePath = "api/now/table/pm_project",
                        Query = [
                            sysparm_display_value = "true",
                            sysparm_exclude_reference_link = "false",
                            sysparm_query = queryFilter,
                            sysparm_limit = "5000"
                        ],
                        Headers = [Accept = "application/json"]
                    ]
                )
            ),
            ResponseTable = Table.FromRecords({response}),
            ExpandedList = Table.ExpandListColumn(ResponseTable, "result"),
            ExpandedRecords = Table.ExpandRecordColumn(ExpandedList, "result", {"short_description","number","u_accounting_master","u_company_code","sys_id","u_project_state","u_project_category","u_bill_code","u_ppm_debtor_relationship","u_profit_center","primary_portfolio", "percent_complete", "project_manager", "u_project_manager_deputee","status", "u_complexity", "demand", "u_project_controller", "u_project_sponsor", "start_date", "end_date","u_az_group_project_id","u_ppm_request_envelope_id","sys_updated_on"}, {"short_description","number", "u_accounting_master","u_company_code","sys_id","u_project_state","u_project_category","u_bill_code","u_ppm_debtor_relationship","u_profit_center","primary_portfolio", "percent_complete", "project_manager", "u_project_manager_deputee","status", "u_complexity", "demand", "u_project_controller", "u_project_sponsor", "start_date", "end_date","u_az_group_project_id","u_ppm_request_envelope_id", "sys_updated_on"})
        in
            ExpandedRecords,
  batches = List.Split(UniqueProjectIds, 50),
  allProjects = Table.Combine(List.Transform(batches, each fetchProjectBatch(_))),
  #"Expanded u_accounting_master" = Table.ExpandRecordColumn(allProjects, "u_accounting_master", {"display_value", "link"}, {"display_value", "link"}),
  #"Renamed Columns" = Table.RenameColumns(#"Expanded u_accounting_master", {{"display_value", "accounting_master"}, {"number", "project_number"}}),
  #"Removed Columns" = Table.RemoveColumns(#"Renamed Columns", {"link"}),
  #"Inserted Text Range" = Table.AddColumn(#"Removed Columns", "Text Range", each Text.Middle([accounting_master], 0, 4), type text),
  #"Renamed Columns1" = Table.RenameColumns(#"Inserted Text Range", {{"Text Range", "u_company_code_am"}}),
  #"Added Custom" = Table.AddColumn(#"Renamed Columns1", "u_company_code_final", each if [u_company_code] = "" then [u_company_code_am] else [u_company_code]),
  #"Removed Columns1" = Table.RemoveColumns(#"Added Custom", {"u_company_code", "u_company_code_am"}),
  #"Renamed Columns2" = Table.RenameColumns(#"Removed Columns1", {{"u_company_code_final", "u_company_code"}}),
  #"Added Custom1" = Table.AddColumn(#"Renamed Columns2", "ExportFor", each CompanyCode),
  #"Expanded primary_portfolio" = Table.ExpandRecordColumn(#"Added Custom1", "primary_portfolio", {"display_value"}, {"display_value"}),
  #"Renamed Columns3" = Table.RenameColumns(#"Expanded primary_portfolio", {{"display_value", "primary_portfolio"}}),
  #"Expanded u_project_controller" = Table.ExpandRecordColumn(#"Renamed Columns3", "u_project_controller", {"display_value"}, {"display_value"}),
  #"Renamed Columns4" = Table.RenameColumns(#"Expanded u_project_controller", {{"display_value", "u_project_controller"}}),
  #"Expanded u_project_manager_deputee" = Table.ExpandRecordColumn(#"Renamed Columns4", "u_project_manager_deputee", {"display_value"}, {"display_value"}),
  #"Renamed Columns5" = Table.RenameColumns(#"Expanded u_project_manager_deputee", {{"display_value", "u_project_manager_deputee"}}),
  #"Expanded demand" = Table.ExpandRecordColumn(#"Renamed Columns5", "demand", {"display_value"}, {"display_value"}),
  #"Renamed Columns6" = Table.RenameColumns(#"Expanded demand", {{"display_value", "demand"}}),
  #"Replaced Errors" = Table.ReplaceErrorValues(#"Renamed Columns6", {{"demand", ""}}),
  #"Expanded project_manager" = Table.ExpandRecordColumn(#"Replaced Errors", "project_manager", {"display_value"}, {"display_value"}),
  #"Renamed Columns7" = Table.RenameColumns(#"Expanded project_manager", {{"display_value", "project_manager"}}),
  #"Transform columns" = Table.TransformColumnTypes(#"Renamed Columns7", {{"short_description", type text}, {"project_number", type text}, {"accounting_master", type text}, {"sys_id", type text}, {"u_project_state", type text}, {"u_project_category", type text}, {"u_bill_code", type text}, {"u_ppm_debtor_relationship", type text}, {"u_profit_center", type text}, {"primary_portfolio", type text}, {"percent_complete", type text}, {"project_manager", type text}, {"u_project_manager_deputee", type text}, {"status", type text}, {"u_complexity", type text}, {"demand", type text}, {"u_project_controller", type text}, {"u_project_sponsor", type text}, {"start_date", type text}, {"end_date", type text}, {"u_az_group_project_id", type text}, {"u_ppm_request_envelope_id", type text}, {"u_company_code", type text}, {"ExportFor", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"short_description", null}, {"project_number", null}, {"accounting_master", null}, {"sys_id", null}, {"u_project_state", null}, {"u_project_category", null}, {"u_bill_code", null}, {"u_ppm_debtor_relationship", null}, {"u_profit_center", null}, {"primary_portfolio", null}, {"percent_complete", null}, {"project_manager", null}, {"u_project_manager_deputee", null}, {"status", null}, {"u_complexity", null}, {"demand", null}, {"u_project_controller", null}, {"u_project_sponsor", null}, {"start_date", null}, {"end_date", null}, {"u_az_group_project_id", null}, {"u_ppm_request_envelope_id", null}, {"u_company_code", null}, {"ExportFor", null}})
in
  #"Replace errors";
shared time_card_interface_set_a = let
  // Define the start and end dates
  startDate = Date.FromText(DateFrom),
  endDate = Date.FromText(DateTo),
  RecordLimit = Number.FromText(NumberOfRecords),
  CompanyCodes = CompanyCode,
  // Function to get data for a specific week
  getDataForWeek = (weekStart as date, weekEnd as date) =>
    let
        weekStartString = Date.ToText(weekStart, "yyyy-MM-dd"),
        weekEndString = Date.ToText(weekEnd, "yyyy-MM-dd"),
        queryFilter = "tc.week_starts_onBETWEEN" & weekStartString & "@" & weekEndString & "^
        tc.state!=Locked^
        tc.total!=0^
        tc.categorySTARTSWITHProject^
        tc.user.user_nameISNOTEMPTY^
        rep_hours!=0^
        tc.top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeIN" & CompanyCodes
        ,
        
        Source = Json.Document(
        Web.Contents(
        "https://aztech.service-now.com",
        [
            RelativePath = "api/now/table/x_ppm_time_card_interface",
            Query = [
                        sysparm_display_value = "true",
                        sysparm_exclude_reference_link = "false",
                        sysparm_query = queryFilter,
                        sysparm_limit = Number.ToText(RecordLimit)
                    ],
                    Headers = [Accept = "application/json"]
                ]
            )
        ),
        #"Converted to Table" = Table.FromRecords({Source}),
        #"Expanded result1" = Table.ExpandListColumn(#"Converted to Table", "result"),
        #"Expanded result3" = Table.ExpandRecordColumn(#"Expanded result1", "result", {"tc", "comments","postingdate","state", "sys_updated_on"}, {"tc", "comments","postingdate","state","sys_updated_on"})
      
    in
        if Table.RowCount(#"Expanded result3") > 0 then #"Expanded result3" else null,
  // Generate list of week start dates
  weekStarts = List.Generate(
        () => startDate,
        each _ <= endDate,
        each Date.AddDays(_, 7)
    ),
  // Generate list of week end dates
  weekEnds = List.Transform(weekStarts, each Date.AddDays(_, 6)),
  // Fetch data for all weeks and combine the results
  results = List.Transform(List.Zip({weekStarts, weekEnds}), each getDataForWeek(_{0}, _{1})),
  combinedResults = Table.Combine(List.RemoveNulls(results)),
  #"Expanded tc" = Table.ExpandRecordColumn(combinedResults, "tc", {"display_value", "link"}, {"tc.display_value", "tc.link"}),
  #"Extracted Text After Delimiter" = Table.TransformColumns(#"Expanded tc", {{"tc.link", each Text.AfterDelimiter(_, "/", 6), type text}}),
  #"Renamed Columns" = Table.RenameColumns(#"Extracted Text After Delimiter", {{"tc.link", "tc_sys_id"}, {"tc.display_value", "week_starts_on"}, {"postingdate", "posting_date"}}),
  #"Added Custom" = Table.AddColumn(#"Renamed Columns", "ExportFor", each CompanyCode),
  #"Transform columns" = Table.TransformColumnTypes(#"Added Custom", {{"week_starts_on", type text}, {"comments", type text}, {"posting_date", type text}, {"state", type text}, {"ExportFor", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"week_starts_on", null}, {"comments", null}, {"posting_date", null}, {"state", null}, {"ExportFor", null}})
in
  #"Replace errors";
shared time_card_interface_set_b = let
  // Define the start and end dates
  startDate = Date.FromText(DateFrom),
  endDate = Date.FromText(DateTo),
  RecordLimit = Number.FromText(NumberOfRecords),
  CompanyCodes = CompanyCode,
  Debtor = OrganizationalEntity,
  // Function to get data for a specific week
  getDataForWeek = (weekStart as date, weekEnd as date) =>
    let
        weekStartString = Date.ToText(weekStart, "yyyy-MM-dd"),
        weekEndString = Date.ToText(weekEnd, "yyyy-MM-dd"),
        queryFilter = "tc.week_starts_onBETWEEN" & weekStartString & "@" & weekEndString & "^
        tc.state!=Locked^
        tc.total!=0^
        tc.categorySTARTSWITHProject^
        tc.user.user_nameISNOTEMPTY^
        tc.user.u_debtorLIKE" & Debtor & "^" &
        "rep_hours!=0^
        tc.top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeNOT IN" & CompanyCodes
        ,
        
        Source = Json.Document(
                                 Web.Contents(
                 "https://aztech.service-now.com",
        [
            RelativePath = "api/now/table/x_ppm_time_card_interface",
            Query = [
                        sysparm_display_value = "true",
                        sysparm_exclude_reference_link = "false",
                        sysparm_query = queryFilter,
                        sysparm_limit = Number.ToText(RecordLimit)
                    ],
                    Headers = [Accept = "application/json"]
                ]
            )
        ),
        #"Converted to Table" = Table.FromRecords({Source}),
        #"Expanded result1" = Table.ExpandListColumn(#"Converted to Table", "result"),
        #"Expanded result3" = Table.ExpandRecordColumn(#"Expanded result1", "result", {"tc", "comments","postingdate","state","sys_updated_on"}, {"tc", "comments","postingdate","state","sys_updated_on"})
      
    in
        if Table.RowCount(#"Expanded result3") > 0 then #"Expanded result3" else null,
  // Generate list of week start dates
  weekStarts = List.Generate(
        () => startDate,
        each _ <= endDate,
        each Date.AddDays(_, 7)
    ),
  // Generate list of week end dates
  weekEnds = List.Transform(weekStarts, each Date.AddDays(_, 6)),
  // Fetch data for all weeks and combine the results
  results = List.Transform(List.Zip({weekStarts, weekEnds}), each getDataForWeek(_{0}, _{1})),
  combinedResults = Table.Combine(List.RemoveNulls(results)),
  #"Expanded tc" = Table.ExpandRecordColumn(combinedResults, "tc", {"display_value", "link"}, {"tc.display_value", "tc.link"}),
  #"Extracted Text After Delimiter" = Table.TransformColumns(#"Expanded tc", {{"tc.link", each Text.AfterDelimiter(_, "/", 6), type text}}),
  #"Renamed Columns" = Table.RenameColumns(#"Extracted Text After Delimiter", {{"tc.link", "tc_sys_id"}, {"tc.display_value", "week_starts_on"}, {"postingdate", "posting_date"}}),
  #"Added Custom" = Table.AddColumn(#"Renamed Columns", "ExportFor", each CompanyCode),
  #"Transform columns" = Table.TransformColumnTypes(#"Added Custom", {{"week_starts_on", type text}, {"comments", type text}, {"posting_date", type text}, {"state", type text}, {"ExportFor", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"week_starts_on", null}, {"comments", null}, {"posting_date", null}, {"state", null}, {"ExportFor", null}})
in
  #"Replace errors";
shared time_card_interface_externals = let
  // Define the start and end dates
  Year = Text.Start(DateTo, 4),
  startDate = Date.FromText(DateFrom),
  endDate = Date.FromText(DateTo),
  RecordLimit = Number.FromText(NumberOfRecords),
  CompanyCodes = CompanyCode,
  Debtor = OrganizationalEntity,
  queryFilter = "timecard.user.u_debtorLIKE" & Debtor & "^" & "hours!=0^documentdateLIKE" & Year,
  Source = Json.Document(
        Web.Contents(
        "https://aztech.service-now.com",
        [
            RelativePath = "api/now/table/x_ppm_ses_data",
            Query = [
                    sysparm_display_value="true",
                    sysparm_exclude_reference_link="false",
                    sysparm_query=queryFilter,
                    sysparm_limit=Number.ToText(RecordLimit)
                ],
                Headers=[Accept="application/json"]
            ]
        )
    ),
  #"Converted to Table" = Table.FromRecords({Source}),
  #"Expanded result1" = Table.ExpandListColumn(#"Converted to Table", "result"),
  #"Expanded result3" = Table.ExpandRecordColumn(#"Expanded result1", "result", {"timecard", "comments", "posting_date", "state", "documentdate", "raterole", "rate_per_hour", "sys_updated_on"}, {"timecard", "comments", "posting_date", "state", "documentdate", "raterole", "rate_per_hour","sys_updated_on"}),
  #"Expanded timecard" = Table.ExpandRecordColumn(#"Expanded result3", "timecard", {"display_value", "link"}, {"display_value", "link"}),
  #"Extracted Text After Delimiter" = Table.TransformColumns(#"Expanded timecard", {{"link", each Text.AfterDelimiter(_, "/", 6), type text}}),
  #"Renamed Columns" = Table.RenameColumns(#"Extracted Text After Delimiter", {{"display_value", "week_starts_on"}, {"link", "tc_sys_id"}}),
  #"Removed Columns" = Table.RemoveColumns(#"Renamed Columns", {"documentdate"}),
  #"Added Custom" = Table.AddColumn(#"Removed Columns", "ExportFor", each CompanyCode),
  #"Transform columns" = Table.TransformColumnTypes(#"Added Custom", {{"week_starts_on", type text}, {"comments", type text}, {"posting_date", type text}, {"state", type text}, {"raterole", type text}, {"rate_per_hour", type text}, {"ExportFor", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"week_starts_on", null}, {"comments", null}, {"posting_date", null}, {"state", null}, {"raterole", null}, {"rate_per_hour", null}, {"ExportFor", null}})
in
  #"Replace errors";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "extrernal_rate_cards_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared extrernal_rate_cards = let
  // Load Purchase Orders (PO)
  startDate = Date.FromText(DateFrom),
  endDate = Date.FromText(DateTo),
  RecordLimit = Number.FromText(NumberOfRecords),
  CompanyCodes = CompanyCode,
  Debtor = OrganizationalEntity,
  poSource = Json.Document(Web.Contents(
        "https://aztech.service-now.com",
        [
            RelativePath = "api/now/table/proc_po",
            Query = [
                sysparm_display_value = "true",
                sysparm_exclude_reference_link = "false",
                sysparm_query = "u_company_codeIN" & CompanyCodes & "^u_demand_idSTARTSWITHG^ORu_demand_idSTARTSWITHR",
                sysparm_limit=Number.ToText(RecordLimit)
            ],
            Headers = [Accept = "application/json"]
        ]
    )),
  poTable = Table.ExpandRecordColumn(
        Table.ExpandListColumn(Table.FromRecords({poSource}), "result"),
        "result", {"sys_id", "number","u_demand_id","u_transfer_status","status","u_vendor_name","u_wbs_element_reference","u_planning_wbs_element", "sys_updated_on"}
    ),
  #"Renamed Columns" = Table.RenameColumns(poTable, {{"sys_id", "po_sys_id"}, {"number", "po_number"}}),
  distinctPoSysIds = List.Distinct(#"Renamed Columns"[po_sys_id]),
  // Load Purchase Order Items
  fetchPoItems = (batch as list) =>
        let
            batchString = Text.Combine(batch, ","),
            queryFilter = "purchase_orderIN" & batchString,
            response = Json.Document(Web.Contents(
                "https://aztech.service-now.com",
                [
                    RelativePath = "api/now/table/proc_po_item",
                    Query = [
                        sysparm_display_value = "true",
                        sysparm_exclude_reference_link = "false",
                        sysparm_query = queryFilter,
                        sysparm_limit = "500"
                    ],
                    Headers = [Accept = "application/json"]
                ]
            )),
            expanded = Table.ExpandRecordColumn(
                Table.ExpandListColumn(Table.FromRecords({response}), "result"),
                "result", {"sys_id", "number", "purchase_order"}
            )
        in
            expanded,
  poBatches = List.Split(distinctPoSysIds, 50),
  poItems = Table.Combine(List.Transform(poBatches, each fetchPoItems(_))),
  #"Expanded purchase_order1" = Table.ExpandRecordColumn(poItems, "purchase_order", {"display_value", "link"}, {"display_value", "link"}),
  #"Extracted Text After Delimiter1" = Table.TransformColumns(#"Expanded purchase_order1", {{"link", each Text.AfterDelimiter(_, "/", 6), type text}}),
  #"Renamed Columns3" = Table.RenameColumns(#"Extracted Text After Delimiter1", {{"link", "po_sys_id"}}),
  #"Renamed Columns1" = Table.RenameColumns(#"Renamed Columns3", {{"sys_id", "pol_sys_id"}, {"number", "pol_number"}}),
  // Get distinct PO Item sys_ids
  distinctPoItemSysIds = List.Distinct(#"Renamed Columns1"[pol_sys_id]),
  // Load External Rate Card by PO Item sys_id
  fetchExtRates = (batch as list) =>
        let
            batchString = Text.Combine(batch, ","),
            queryFilter = "purchase_orderIN" & batchString,
            response = Json.Document(Web.Contents(
                "https://aztech.service-now.com",
                [
                    RelativePath = "api/now/table/x_ppm_external_rate_card",
                    Query = [
                        sysparm_display_value = "true",
                        sysparm_exclude_reference_link = "false",
                        sysparm_query = queryFilter,
                        sysparm_limit = "500"
                    ],
                    Headers = [Accept = "application/json"]
                ]
            )),
            expanded = Table.ExpandRecordColumn(
                Table.ExpandRecordColumn(
                    Table.ExpandRecordColumn(
                        Table.ExpandListColumn(Table.FromRecords({response}), "result"),
                        "result", {"sys_id", "purchase_order", "rate_card", "user","active"}
                    ),
                    "rate_card", {"display_value","link"}, {"rate_card_display","rl link"}
                ),
                "user", {"display_value","link"}, {"user_display","user link"}
            )
        in
            expanded,
  extBatches = List.Split(distinctPoItemSysIds, 50),
  extRates = Table.Combine(List.Transform(extBatches, each fetchExtRates(_))),
  #"Renamed Columns4" = Table.RenameColumns(extRates, {{"rl link", "rl_sys_id"}, {"user link", "user_sys_id"}}),
  #"Extracted Text After Delimiter2" = Table.TransformColumns(#"Renamed Columns4", {{"rl_sys_id", each Text.AfterDelimiter(_, "/", 6), type text}}),
  #"Extracted Text After Delimiter3" = Table.TransformColumns(#"Extracted Text After Delimiter2", {{"user_sys_id", each Text.AfterDelimiter(_, "/", 6), type text}}),
  #"Expanded purchase_order" = Table.ExpandRecordColumn(#"Extracted Text After Delimiter3", "purchase_order", {"display_value", "link"}, {"display_value", "link"}),
  #"Extracted Text After Delimiter" = Table.TransformColumns(#"Expanded purchase_order", {{"link", each Text.AfterDelimiter(_, "/", 6), type text}}),
  #"Renamed Columns2" = Table.RenameColumns(#"Extracted Text After Delimiter", {{"link", "pol_sys_id"}}),
  // Join all three tables
  joinPOtoItems = Table.NestedJoin(#"Renamed Columns", "po_sys_id", #"Renamed Columns1", "po_sys_id", "po_items", JoinKind.LeftOuter),
  #"Expanded po_items" = Table.ExpandTableColumn(joinPOtoItems, "po_items", {"pol_sys_id", "pol_number", "display_value", "po_sys_id"}, {"pol_sys_id", "pol_number", "display_value", "po_sys_id.1"}),
  joinPOItemsToRates = Table.NestedJoin(#"Expanded po_items", "pol_sys_id", #"Renamed Columns2", "pol_sys_id", "rate_data", JoinKind.LeftOuter),
  #"Expanded rate_data" = Table.ExpandTableColumn(joinPOItemsToRates, "rate_data", {"rate_card_display", "rl_sys_id", "user_display", "user_sys_id", "active"}, {"rate_card_display", "rl_sys_id", "user_display", "user_sys_id", "active"}),
  // Get distinct rl_sys_ids for fetching u_rate_line
  distinctRlSysIds = List.Distinct(#"Expanded rate_data"[rl_sys_id]),
  // Fetch u_rate_line by rl_sys_id in batches
  fetchRateLines = (batch as list) =>
    let
        batchString = Text.Combine(batch, ","),
        queryFilter = "sys_idIN" & batchString,
        response = Json.Document(Web.Contents(
            "https://aztech.service-now.com",
            [
                RelativePath = "api/now/table/u_rate_line",
                Query = [
                    sysparm_display_value = "true",
                    sysparm_exclude_reference_link = "false",
                    sysparm_query = queryFilter,
                    sysparm_limit = "500"
                ],
                Headers = [Accept = "application/json"]
            ]
        )),
        expanded = Table.ExpandListColumn(Table.FromRecords({response}), "result"),
        finalTable = Table.ExpandRecordColumn(expanded, "result", {"sys_id", "u_rate_per_hour"})
    in
        Table.RenameColumns(finalTable, {{"sys_id", "rl_sys_id"}}),
  // Important for join
  // Batch and combine u_rate_line
  rlBatches = List.Split(distinctRlSysIds, 50),
  rateLines = Table.Combine(List.Transform(rlBatches, each fetchRateLines(_))),
  fx_rate_raw = Json.Document(
    Web.Contents(
        "https://aztech.service-now.com",
        [
            RelativePath = "api/now/table/fx_rate",
            Query = [
                sysparm_display_value = "true",
                sysparm_exclude_reference_link = "false",
                sysparm_query = "currency.symbolSTARTSWITHusd^ORDERBYDESCsys_created_on",
                sysparm_limit = "1"
            ],
            Headers = [Accept = "application/json"]
        ]
    )
),
  fx_rate_table = Table.ExpandRecordColumn(
    Table.ExpandListColumn(Table.FromRecords({fx_rate_raw}), "result"),
    "result", {"rate"}
),
  fx_rate_value = fx_rate_table{0}[rate],
  // Final join with rate lines
  finalJoin = Table.NestedJoin(#"Expanded rate_data", "rl_sys_id", rateLines, "rl_sys_id", "rate_lines", JoinKind.LeftOuter),
  #"Expanded rate_lines" = Table.ExpandTableColumn(finalJoin, "rate_lines", {"rl_sys_id", "u_rate_per_hour"}, {"rl_sys_id.1", "u_rate_per_hour"}),
  #"Replaced Value" = Table.ReplaceValue(#"Expanded rate_lines", "USD", "", Replacer.ReplaceText, {"u_rate_per_hour"}),
  #"Changed Type" = Table.TransformColumnTypes(#"Replaced Value", {{"u_rate_per_hour", Currency.Type}}),
  #"Added fx_rate" = Table.AddColumn(#"Changed Type", "fx_rate_usd", each fx_rate_value),
  #"Changed Type1" = Table.TransformColumnTypes(#"Added fx_rate", {{"fx_rate_usd", Currency.Type}}),
  #"Added Custom" = Table.AddColumn(#"Changed Type1", "rate_per_hour_eur", each [u_rate_per_hour] / [fx_rate_usd]),
  #"Expanded u_wbs_element_reference" = Table.ExpandRecordColumn(#"Added Custom", "u_wbs_element_reference", {"display_value"}, {"display_value.1"}),
  #"Replaced Errors" = Table.ReplaceErrorValues(#"Expanded u_wbs_element_reference", {{"display_value.1", ""}}),
  #"Renamed Columns5" = Table.RenameColumns(#"Replaced Errors", {{"display_value.1", "u_wbs_element"}}),
  #"Expanded u_planning_wbs_element" = Table.ExpandRecordColumn(#"Renamed Columns5", "u_planning_wbs_element", {"display_value"}, {"display_value.1"}),
  #"Replaced Errors1" = Table.ReplaceErrorValues(#"Expanded u_planning_wbs_element", {{"display_value.1", ""}}),
  #"Renamed Columns6" = Table.RenameColumns(#"Replaced Errors1", {{"display_value.1", "u_planning_wbs_element"}}),
  #"Removed Columns" = Table.RemoveColumns(#"Renamed Columns6", {"po_sys_id.1", "rl_sys_id.1", "po_sys_id", "u_rate_per_hour", "fx_rate_usd"}),
  #"Added Custom1" = Table.AddColumn(#"Removed Columns", "ExportFor", each CompanyCode),
  #"Transform columns" = Table.TransformColumnTypes(#"Added Custom1", {{"po_number", type text}, {"u_demand_id", type text}, {"u_transfer_status", type text}, {"status", type text}, {"u_vendor_name", type text}, {"u_wbs_element", type text}, {"u_planning_wbs_element", type text}, {"pol_sys_id", type text}, {"pol_number", type text}, {"display_value", type text}, {"rate_card_display", type text}, {"user_display", type text}, {"ExportFor", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"po_number", null}, {"u_demand_id", null}, {"u_transfer_status", null}, {"status", null}, {"u_vendor_name", null}, {"u_wbs_element", null}, {"u_planning_wbs_element", null}, {"pol_sys_id", null}, {"pol_number", null}, {"display_value", null}, {"rate_card_display", null}, {"user_display", null}, {"ExportFor", null}}),
  #"Transform columns 1" = Table.TransformColumnTypes(#"Replace errors", {{"rate_per_hour_eur", type text}}),
  #"Replace errors 1" = Table.ReplaceErrorValues(#"Transform columns 1", {{"rate_per_hour_eur", null}}),
  #"Filtered rows" = Table.SelectRows(#"Replace errors 1", each [active] = "true"),
  #"Transform columns 2" = Table.TransformColumnTypes(#"Filtered rows", {{"active", type text}}),
  #"Replace errors 2" = Table.ReplaceErrorValues(#"Transform columns 2", {{"active", null}})
in
  #"Replace errors 2";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "time_cards_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "u_project_name", DestinationColumnName = "u_project_name"], [SourceColumnName = "project_number", DestinationColumnName = "project_number"], [SourceColumnName = "total", DestinationColumnName = "total"], [SourceColumnName = "week_starts_on", DestinationColumnName = "week_starts_on"], [SourceColumnName = "monday", DestinationColumnName = "monday"], [SourceColumnName = "tuesday", DestinationColumnName = "tuesday"], [SourceColumnName = "wednesday", DestinationColumnName = "wednesday"], [SourceColumnName = "thursday", DestinationColumnName = "thursday"], [SourceColumnName = "friday", DestinationColumnName = "friday"], [SourceColumnName = "saturday", DestinationColumnName = "saturday"], [SourceColumnName = "sunday", DestinationColumnName = "sunday"], [SourceColumnName = "category", DestinationColumnName = "category"], [SourceColumnName = "state", DestinationColumnName = "state"], [SourceColumnName = "u_dept_cost_center", DestinationColumnName = "u_dept_cost_center"], [SourceColumnName = "u_prj_unit_resp", DestinationColumnName = "u_prj_unit_resp"], [SourceColumnName = "project_sys_id", DestinationColumnName = "project_sys_id"], [SourceColumnName = "u_timecard_source", DestinationColumnName = "u_timecard_source"], [SourceColumnName = "u_rate_role", DestinationColumnName = "u_rate_role"], [SourceColumnName = "u_project_manager", DestinationColumnName = "u_project_manager"], [SourceColumnName = "u_resource_manager", DestinationColumnName = "u_resource_manager"], [SourceColumnName = "resource_plan", DestinationColumnName = "resource_plan"], [SourceColumnName = "user", DestinationColumnName = "user"], [SourceColumnName = "user_sys_id", DestinationColumnName = "user_sys_id"], [SourceColumnName = "tc_sys_id", DestinationColumnName = "tc_sys_id"], [SourceColumnName = "sys_updated_by", DestinationColumnName = "sys_updated_by"], [SourceColumnName = "sys_updated_on", DestinationColumnName = "sys_updated_on"], [SourceColumnName = "user_name", DestinationColumnName = "user_name"], [SourceColumnName = "rate", DestinationColumnName = "rate"], [SourceColumnName = "ExportFor", DestinationColumnName = "ExportFor"], [SourceColumnName = "RefreshDate", DestinationColumnName = "RefreshDate"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared time_cards = let
  // Parameters
  startDate = Date.FromText(DateFrom),
  endDate = Date.FromText(DateTo),
  RecordLimit = Number.FromText(NumberOfRecords),
  CompanyCodes = CompanyCode,
  Debtor = OrganizationalEntity,
  // Generate weekly ranges
  weekStarts = List.Generate(() => startDate, each _ <= endDate, each Date.AddDays(_, 7)),
  weekEnds = List.Transform(weekStarts, each Date.AddDays(_, 6)),
  // Fetch time_card for a given week and filter
  fetchTimeCards = (weekStart as date, weekEnd as date, filterText as text) =>
    let
        filter = "week_starts_onBETWEEN" & Date.ToText(weekStart, "yyyy-MM-dd") & "@" & Date.ToText(weekEnd, "yyyy-MM-dd") & "^" & filterText,
        response = Json.Document(Web.Contents("https://aztech.service-now.com", [
            RelativePath = "api/now/table/time_card",
            Query = [
                sysparm_display_value = "true",
                sysparm_exclude_reference_link = "false",
                sysparm_query = filter,
                sysparm_limit = Number.ToText(RecordLimit)
            ],
            Headers = [Accept = "application/json"]
        ])),
        tbl = Table.FromRecords({response}),
        expanded = Table.ExpandListColumn(tbl, "result"),
        records = Table.ExpandRecordColumn(expanded, "result", {"u_project_name","top_task","total", "week_starts_on","monday","tuesday","wednesday","thursday","friday","saturday","sunday", "category","state","u_dept_cost_center","u_prj_unit_resp","u_rep_project","u_timecard_source","u_rate_role","u_project_manager","u_resource_manager","resource_plan","user","sys_id","sys_updated_by", "sys_updated_on"}, {"u_project_name","top_task", "total", "week_starts_on","monday","tuesday","wednesday","thursday","friday","saturday","sunday","category", "state","u_dept_cost_center","u_prj_unit_resp","u_rep_project","u_timecard_source","u_rate_role","u_project_manager","u_resource_manager","resource_plan","user","tc_sys_id","sys_updated_by", "sys_updated_on"}),
        userExpanded = Table.ExpandRecordColumn(records, "user", {"display_value", "link"}, {"user", "link"}),
        parsed = Table.TransformColumns(userExpanded, {{"link", each Text.AfterDelimiter(_, "/", 6), type text}}),
        renamed = Table.RenameColumns(parsed, {{"link", "user_sys_id"}})
    in
        if Table.IsEmpty(renamed) then null else renamed,
  // Load and combine time cards
  timeCardLoad = (baseFilter as text) =>
        Table.Combine(List.RemoveNulls(List.Transform(List.Zip({weekStarts, weekEnds}), each fetchTimeCards(_{0}, _{1}, baseFilter)))),
  timeCardSetA = timeCardLoad("state!=Locked^total!=0^categorySTARTSWITHProject^top_task.ref_pm_project.u_accounting_master.u_master_approved!=Rejected^top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeIN" & CompanyCodes),
  timeCardSetB = timeCardLoad("state!=Locked^total!=0^categorySTARTSWITHProject^top_task.ref_pm_project.u_accounting_master.u_master_approved!=Rejected^user.user_nameISNOTEMPTY^" & "user.u_debtorLIKE" & Debtor & "^top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeNOT IN" & CompanyCodes),
  timeCardSetC = timeCardLoad("state!=Locked^total!=0^categoryNOT LIKEProject^user.user_nameISNOTEMPTY^" & "user.u_debtorLIKE" & Debtor),
  allTimeCards = Table.Combine({timeCardSetA, timeCardSetB, timeCardSetC}),
  distinctUserSysIds = List.Distinct(allTimeCards[user_sys_id]),
  // Fetch sys_user by batch
  fetchUsers = (batch as list) =>
    let
        query = "sys_idIN" & Text.Combine(batch, ","),
        response = Json.Document(Web.Contents("https://aztech.service-now.com", [
            RelativePath = "api/now/table/sys_user",
            Query = [
                sysparm_display_value = "true",
                sysparm_exclude_reference_link = "false",
                sysparm_query = query,
                sysparm_limit = "5000"
            ],
            Headers = [Accept = "application/json"]
        ])),
        tbl = Table.FromRecords({response}),
        expanded = Table.ExpandListColumn(tbl, "result"),
        final1 = Table.ExpandRecordColumn(expanded, "result", {"user_name", "sys_id"})
    in
        final1,
  userBatches = List.Split(distinctUserSysIds, 50),
  allUsers = Table.Combine(List.Transform(userBatches, each fetchUsers(_))),
  // Fetch resource rate cards
  fetchRateCards = (batch as list) =>
    let
        query = "user.sys_idIN" & Text.Combine(batch, ","),
        response = Json.Document(Web.Contents("https://aztech.service-now.com", [
            RelativePath = "api/now/table/x_ppm_resource_rate_card",
            Query = [
                sysparm_display_value = "true",
                sysparm_exclude_reference_link = "false",
                sysparm_fields = "user,co_rate_role,active,sys_created_on",
                sysparm_query = query,
                sysparm_limit = "5000"
            ],
            Headers = [Accept = "application/json"]
        ])),
        tbl = Table.FromRecords({response}),
        expandedList = Table.ExpandListColumn(tbl, "result"),
        expanded = Table.ExpandRecordColumn(expandedList, "result", {"user", "co_rate_role", "active", "sys_created_on"}, {"user", "co_rate_role", "active", "sys_created_on"}),
        roleExpanded = Table.ExpandRecordColumn(expanded, "co_rate_role", {"display_value"}, {"rate"}),
        userExpanded = Table.ExpandRecordColumn(roleExpanded, "user", {"link"}, {"user_link"}),
        extracted = Table.TransformColumns(userExpanded, {{"user_link", each Text.AfterDelimiter(_, "/", 6), type text}}),
        renamed = Table.RenameColumns(extracted, {{"user_link", "sys_id"}}),
        removedActive = Table.RemoveColumns(renamed, {"active"}),
        sorted = Table.Sort(removedActive, {{"sys_id", Order.Ascending}, {"sys_created_on", Order.Descending}}),
        grouped = Table.Group(sorted, {"sys_id"}, {{"TopRecord", each Table.FirstN(_, 1)}}),
        final2 = Table.ExpandTableColumn(grouped, "TopRecord", {"rate", "sys_created_on"})
    in
        final2,
  rateCardBatches = List.Split(distinctUserSysIds, 50),
  allRateCards = Table.Combine(List.Transform(rateCardBatches, each fetchRateCards(_))),
  allRateCardsClean = Table.RenameColumns(Table.Distinct(allRateCards, {"sys_id"}), {{"sys_id", "rate_sys_id"}}),
  // Combine all data
  joinWithUsers = Table.Join(allTimeCards, "user_sys_id", allUsers, "sys_id", JoinKind.LeftOuter),
  joinWithUsersClean = Table.RemoveColumns(joinWithUsers, {"sys_id"}),
  joinWithRates = Table.Join(joinWithUsersClean, "user_sys_id", allRateCardsClean, "rate_sys_id", JoinKind.LeftOuter),
  joinWithRatesClean = Table.RemoveColumns(joinWithRates, {"sys_created_on", "rate_sys_id"}),
  withExportInfo = Table.AddColumn(joinWithRatesClean, "ExportFor", each CompanyCode),
  #"Added Custom" = Table.AddColumn(withExportInfo, "RefreshDate", each DateTime.LocalNow()),
  #"Expanded u_dept_cost_center" = Table.ExpandRecordColumn(#"Added Custom", "u_dept_cost_center", {"display_value", "link"}, {"u_dept_cost_center.display_value", "u_dept_cost_center.link"}),
  #"Replaced Errors" = Table.ReplaceErrorValues(#"Expanded u_dept_cost_center", {{"u_dept_cost_center.display_value", ""}}),
  #"Removed Columns1" = Table.RemoveColumns(#"Replaced Errors", {"u_dept_cost_center.link"}),
  #"Expanded u_rep_project" = Table.ExpandRecordColumn(#"Removed Columns1", "u_rep_project", {"display_value", "link"}, {"u_rep_project.display_value", "u_rep_project.link"}),
  #"Extracted Text After Delimiter" = Table.TransformColumns(#"Expanded u_rep_project", {{"u_rep_project.link", each Text.AfterDelimiter(_, "/", 6), type text}}),
  #"Renamed Columns" = Table.RenameColumns(#"Extracted Text After Delimiter", {{"u_rep_project.link", "project_sys_id"}}),
  #"Replaced Errors1" = Table.ReplaceErrorValues(#"Renamed Columns", {{"project_sys_id", ""}}),
  #"Expanded u_project_manager" = Table.ExpandRecordColumn(#"Replaced Errors1", "u_project_manager", {"display_value", "link"}, {"display_value", "link"}),
  #"Renamed Columns2" = Table.RenameColumns(#"Expanded u_project_manager", {{"display_value", "u_project_manager"}}),
  #"Removed Columns" = Table.RemoveColumns(#"Renamed Columns2", {"link"}),
  #"Replaced Errors2" = Table.ReplaceErrorValues(#"Removed Columns", {{"u_project_manager", ""}}),
  #"Renamed Columns1" = Table.RenameColumns(#"Replaced Errors2", {{"u_dept_cost_center.display_value", "u_dept_cost_center"}}),
  #"Removed Columns2" = Table.RemoveColumns(#"Renamed Columns1", {"u_rep_project.display_value"}),
  #"Expanded u_prj_unit_resp" = Table.ExpandRecordColumn(#"Removed Columns2", "u_prj_unit_resp", {"display_value", "link"}, {"display_value", "link"}),
  #"Renamed Columns3" = Table.RenameColumns(#"Expanded u_prj_unit_resp", {{"display_value", "u_prj_unit_resp"}}),
  #"Removed Columns3" = Table.RemoveColumns(#"Renamed Columns3", {"link"}),
  #"Replaced Errors3" = Table.ReplaceErrorValues(#"Removed Columns3", {{"u_prj_unit_resp", ""}}),
  #"Expanded u_resource_manager" = Table.ExpandRecordColumn(#"Replaced Errors3", "u_resource_manager", {"display_value", "link"}, {"display_value", "link"}),
  #"Removed Columns4" = Table.RemoveColumns(#"Expanded u_resource_manager", {"link"}),
  #"Renamed Columns4" = Table.RenameColumns(#"Removed Columns4", {{"display_value", "u_resource_manager"}}),
  #"Expanded resource_plan" = Table.ExpandRecordColumn(#"Renamed Columns4", "resource_plan", {"display_value", "link"}, {"display_value", "link"}),
  #"Removed Columns5" = Table.RemoveColumns(#"Expanded resource_plan", {"link"}),
  #"Renamed Columns5" = Table.RenameColumns(#"Removed Columns5", {{"display_value", "resource_plan"}}),
  #"Replaced Errors4" = Table.ReplaceErrorValues(#"Renamed Columns5", {{"resource_plan", ""}}),
  #"Expanded top_task" = Table.ExpandRecordColumn(#"Replaced Errors4", "top_task", {"display_value", "link"}, {"display_value", "link"}),
  #"Removed Columns6" = Table.RemoveColumns(#"Expanded top_task", {"link"}),
  #"Replaced Errors5" = Table.ReplaceErrorValues(#"Removed Columns6", {{"display_value", ""}}),
  #"Renamed Columns6" = Table.RenameColumns(#"Replaced Errors5", {{"display_value", "project_number"}}),
  #"Transform columns" = Table.TransformColumnTypes(#"Renamed Columns6", {{"u_project_name", type text}, {"project_number", type text}, {"total", type text}, {"week_starts_on", type text}, {"monday", type text}, {"tuesday", type text}, {"wednesday", type text}, {"thursday", type text}, {"friday", type text}, {"saturday", type text}, {"sunday", type text}, {"category", type text}, {"state", type text}, {"u_dept_cost_center", type text}, {"u_prj_unit_resp", type text}, {"u_timecard_source", type text}, {"u_rate_role", type text}, {"u_project_manager", type text}, {"u_resource_manager", type text}, {"resource_plan", type text}, {"user", type text}, {"tc_sys_id", type text}, {"sys_updated_by", type text}, {"sys_updated_on", type text}, {"user_name", type text}, {"rate", type text}, {"ExportFor", type text}, {"RefreshDate", type text}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Transform columns", {{"u_project_name", null}, {"project_number", null}, {"total", null}, {"week_starts_on", null}, {"monday", null}, {"tuesday", null}, {"wednesday", null}, {"thursday", null}, {"friday", null}, {"saturday", null}, {"sunday", null}, {"category", null}, {"state", null}, {"u_dept_cost_center", null}, {"u_prj_unit_resp", null}, {"u_timecard_source", null}, {"u_rate_role", null}, {"u_project_manager", null}, {"u_resource_manager", null}, {"resource_plan", null}, {"user", null}, {"tc_sys_id", null}, {"sys_updated_by", null}, {"sys_updated_on", null}, {"user_name", null}, {"rate", null}, {"ExportFor", null}, {"RefreshDate", null}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Replace errors", {{"sys_updated_on", type datetime}})
in
    #"Changed column type";
shared DateFrom = "2023-12-31" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared DateTo = "2023-12-31" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared NumberOfRecords = "1" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared OrganizationalEntity = "2000014578 Allianz Technology s.r.o." meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type any, DefaultValue = ""];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "time_card_interface_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared time_card_interface = let
  Source = Table.Combine({time_card_interface_externals, time_card_interface_set_a, time_card_interface_set_b})
in
  Source;
shared time_card_interface_table = "2024_cz45_time_card_interface" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared time_card_interface_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data],
  TableNavigation = Navigation_2{[Id = time_card_interface_table, ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared u_ppm_accounting_element_table = "2024_cz45_u_ppm_accounting_element" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared u_ppm_accounting_element_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data],
  TableNavigation = Navigation_2{[Id = u_ppm_accounting_element_table, ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared pm_projects_table = "2024_cz45_pm_projects" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared extrernal_rate_cards_table = "2024_cz45_extrernal_rate_cards" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared time_cards_table = "2024_cz45_time_cards" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared pm_projects_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data],
  TableNavigation = Navigation_2{[Id = pm_projects_table, ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared extrernal_rate_cards_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data],
  TableNavigation = Navigation_2{[Id = extrernal_rate_cards_table, ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared time_cards_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data],
  TableNavigation = Navigation_2{[Id = "2024_hu44_time_cards", ItemKind = "Table"]}[Data]
in
  TableNavigation;
