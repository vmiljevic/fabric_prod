section Section1;
shared CompanyCode = "AT45" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared NumberOfRecords = "9999" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared Debtor = "9d2a756969c65200e4854a54f2f9271a" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared OrgEntity = "0" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared tn_accounting_element = "dim_ppm_sk44_accounting_element" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared tn_project = "fact_ppm_sk44_project" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared tn_external_rate_cards = "dim_ppm_sk44_external_rate_cards" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
shared tn_sys_users = "dim_ppm_sk44_sys_users" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text, DefaultValue = ""];
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
  #"Expanded result3" = Table.ExpandRecordColumn(#"Expanded result1", "result", {"u_accounting_master", "u_wbs_element_id", "u_element_approved", "u_expense_type", "u_name","u_project"}, {"u_accounting_master", "u_wbs_element_id", "u_element_approved", "u_expense_type", "u_name","u_project"}),
  #"Expanded u_accounting_master" = Table.ExpandRecordColumn(#"Expanded result3", "u_accounting_master", {"display_value", "link"}, {"display_value", "link"}),
  #"Renamed Columns" = Table.RenameColumns(#"Expanded u_accounting_master", {{"display_value", "accounting_master"}}),
  #"Removed Columns" = Table.RemoveColumns(#"Renamed Columns", {"link"}),
  #"Added Custom" = Table.AddColumn(#"Removed Columns", "ExportFor", each CompanyCode),
  #"Expanded u_project" = Table.ExpandRecordColumn(#"Added Custom", "u_project", {"display_value", "link"}, {"display_value", "link"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Expanded u_project", {{"display_value", "u_project"}, {"link", "project_sys_id"}}),
  #"Split column by delimiter" = Table.SplitColumn(Table.TransformColumnTypes(#"Renamed columns 1", {{"project_sys_id", type text}}), "project_sys_id", Splitter.SplitTextByEachDelimiter({"/"}, QuoteStyle.Csv, true), {"project_sys_id.1", "project_sys_id.2"}),
  #"Removed columns 1" = Table.RemoveColumns(#"Split column by delimiter", {"project_sys_id.1"}),
  #"Renamed columns 2" = Table.RenameColumns(#"Removed columns 1", {{"project_sys_id.2", "project_sys_id"}}),
  #"Replace errors" = Table.ReplaceErrorValues(#"Renamed columns 2", {{"accounting_master", null}, {"u_wbs_element_id", null}, {"u_element_approved", null}, {"u_expense_type", null}, {"u_name", null}, {"ExportFor", null}, {"u_project", null}, {"project_sys_id", null}}),
  #"Transform columns" = Table.TransformColumnTypes(#"Replace errors", {{"accounting_master", type text}, {"u_wbs_element_id", type text}, {"u_element_approved", type text}, {"u_expense_type", type text}, {"u_name", type text}, {"ExportFor", type text}, {"u_project", type text}})
in
  #"Transform columns";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "pm_projects_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared pm_projects = let
  // Step 1: Get list of project IDs from interface

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
            ExpandedRecords = Table.ExpandRecordColumn(ExpandedList, "result", {"short_description","number","u_accounting_master","u_company_code","sys_id","u_project_state","u_project_category","u_bill_code","u_ppm_debtor_relationship","u_profit_center","primary_portfolio", "percent_complete", "project_manager", "u_project_manager_deputee","status", "u_complexity", "demand", "u_project_controller", "u_project_sponsor", "start_date", "end_date","u_az_group_project_id","u_ppm_request_envelope_id"}, {"short_description","number", "u_accounting_master","u_company_code","sys_id","u_project_state","u_project_category","u_bill_code","u_ppm_debtor_relationship","u_profit_center","primary_portfolio", "percent_complete", "project_manager", "u_project_manager_deputee","status", "u_complexity", "demand", "u_project_controller", "u_project_sponsor", "start_date", "end_date","u_az_group_project_id","u_ppm_request_envelope_id"})
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
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "extrernal_rate_cards_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared extrernal_rate_cards = let
  // Load Purchase Orders (PO)
  RecordLimit = Number.FromText(NumberOfRecords),
  CompanyCodes = CompanyCode,
  poSource = Json.Document(Web.Contents(
        "https://aztech.service-now.com",
        [
            RelativePath = "api/now/table/proc_po",
            Query = [
                sysparm_display_value = "true",
                sysparm_exclude_reference_link = "false",
                sysparm_query = "u_company_codeIN" & CompanyCodes & "^u_demand_idSTARTSWITHR^ORu_demand_idSTARTSWITHG",
                sysparm_limit=Number.ToText(RecordLimit)
            ],
            Headers = [Accept = "application/json"]
        ]
    )),
  poTable = Table.ExpandRecordColumn(
        Table.ExpandListColumn(Table.FromRecords({poSource}), "result"),
        "result", {"sys_id", "number","u_demand_id","u_transfer_status","status","u_vendor_name","u_wbs_element_reference","u_planning_wbs_element"}
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
                "result", {"sys_id", "number", "purchase_order","u_purchase_no"}
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
                        "result", {"sys_id", "purchase_order", "rate_card", "user"}
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
  #"Expanded po_items" = Table.ExpandTableColumn(joinPOtoItems, "po_items", {"pol_sys_id", "pol_number", "display_value", "po_sys_id","u_purchase_no"}, {"pol_sys_id", "pol_number", "display_value", "po_sys_id.1","u_purchase_no"}),
  joinPOItemsToRates = Table.NestedJoin(#"Expanded po_items", "pol_sys_id", #"Renamed Columns2", "pol_sys_id", "rate_data", JoinKind.LeftOuter),
  #"Expanded rate_data" = Table.ExpandTableColumn(joinPOItemsToRates, "rate_data", {"rate_card_display", "rl_sys_id", "user_display", "user_sys_id"}, {"rate_card_display", "rl_sys_id", "user_display", "user_sys_id"}),
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
  #"Changed column type" = Table.TransformColumnTypes(#"Replace errors 1", {{"u_purchase_no", type text}}),
  #"Replaced errors 1" = Table.ReplaceErrorValues(#"Changed column type", {{"u_purchase_no", null}})
in
  #"Replaced errors 1";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "sys_users_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared sys_users = let
  Debtor = Debtor,
  OrgEntity = OrgEntity,
  Source = Json.Document(
            Web.Contents(
                "https://aztech.service-now.com/api/now/table/sys_user",
                [
                    Query = [
                        sysparm_display_value="true",
                        sysparm_exclude_reference_link="false",
                        sysparm_fields="sys_id,user_name, u_legal_entity,employee_number,last_name,first_name,u_organizational_entity,u_local_id,u_employment_id,name,email,u_local_logon_name,u_preferred_logon_name",
                        sysparm_query="u_organizational_entity.sys_id=" & OrgEntity & "^ORu_debtor.sys_id=" & Debtor,
                         sysparm_limit=Number.ToText(50000)
                    ],
                    Headers=[Accept="application/json"]
                ]
            )
        ),
  #"Converted to Table" = Table.FromRecords({Source}),
  #"Expanded result" = Table.ExpandListColumn(#"Converted to Table", "result"),
  #"Expanded result1" = Table.ExpandRecordColumn(#"Expanded result", "result", {"u_local_logon_name", "u_legal_entity", "user_name","sys_id", "u_organizational_entity", "u_employment_id", "u_local_id", "name", "employee_number", "last_name", "first_name", "email", "u_preferred_logon_name"}, {"u_local_logon_name", "u_legal_entity", "user_name","sys_id","u_organizational_entity", "u_employment_id", "u_local_id", "name", "employee_number", "last_name", "first_name", "email", "u_preferred_logon_name"}),
  #"Filtered Rows" = Table.SelectRows(#"Expanded result1", each [u_legal_entity] <> ""),
  #"Expanded u_organizational_entity" = Table.ExpandRecordColumn(#"Filtered Rows", "u_organizational_entity", {"display_value"}, {"display_value"}),
  #"Expanded u_legal_entity" = Table.ExpandRecordColumn(#"Expanded u_organizational_entity", "u_legal_entity", {"display_value"}, {"display_value.1"}),
  #"Renamed Columns" = Table.RenameColumns(#"Expanded u_legal_entity", {{"display_value.1", "legal_entity"}, {"display_value", "organizational_entity"}}),
  #"Added Custom" = Table.AddColumn(#"Renamed Columns", "Export for", each CompanyCode),
  #"Replace errors" = Table.ReplaceErrorValues(#"Added Custom", {{"u_local_logon_name", null}, {"legal_entity", null}, {"user_name", null},{"sys_id", null}, {"organizational_entity", null}, {"u_employment_id", null}, {"u_local_id", null}, {"name", null}, {"employee_number", null}, {"last_name", null}, {"first_name", null}, {"email", null}, {"Export for", null}, {"u_preferred_logon_name",null}}),
  #"Transform columns" = Table.TransformColumnTypes(#"Replace errors", {{"u_local_logon_name", type text}, {"legal_entity", type text}, {"user_name", type text}, {"organizational_entity", type text}, {"u_employment_id", type text}, {"u_local_id", type text}, {"name", type text}, {"employee_number", type text}, {"last_name", type text}, {"sys_id", type text}, {"first_name", type text}, {"email", type text}, {"Export for", type text}, {"u_preferred_logon_name", type text}})
in
  #"Transform columns";
shared u_ppm_accounting_element_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = tn_accounting_element, ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared pm_projects_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = tn_project, ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared extrernal_rate_cards_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = tn_external_rate_cards, ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared sys_users_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = Variable.ValueOrDefault("$(/**/vl_main/WorkspaceID)",  "c70edda0-25b6-4c4e-ad60-5a2466e1851f")]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = Variable.ValueOrDefault("$(/**/vl_main/SilverLakehouseID)",  "d6534641-b8b1-49f6-b4c4-5902e79b3a2b")]}[Data],
  TableNavigation = Navigation_2{[Id = tn_sys_users, ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
