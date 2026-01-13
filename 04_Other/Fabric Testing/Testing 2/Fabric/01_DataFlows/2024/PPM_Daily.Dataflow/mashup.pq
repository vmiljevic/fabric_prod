[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared CompanyCode = "AT45" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type any];
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
                    sysparm_query="u_wbs_element_idSTARTSWITH" & CompanyCodes & "^sys_updated_onBETWEENjavascript:gs.dateGenerate("& UpdatedOn &",'00:00:00')@javascript:gs.dateGenerate(" & UpdatedOn & ",'23:59:59')",
                    sysparm_limit=Number.ToText(RecordLimit)
                ],
                Headers=[Accept="application/json"]
            ]
        )
    ),
  #"Converted to Table" = Table.FromRecords({Source}),
  #"Expanded result1" = Table.ExpandListColumn(#"Converted to Table", "result"),
  #"Expanded result3" = Table.ExpandRecordColumn(#"Expanded result1", "result", {"u_accounting_master", "u_wbs_element_id", "u_element_approved", "u_expense_type", "u_name"}, {"u_accounting_master", "u_wbs_element_id", "u_element_approved", "u_expense_type", "u_name"}),
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
  // Parameters
  UpdatedOn = UpdatedOn,
  RecordLimit = Number.FromText(NumberOfRecords),
  CompanyCodes = CompanyCode,

  // Step 1: Get list of project IDs from interface
  RawSource = Json.Document(
    Web.Contents(
      "https://aztech.service-now.com",
      [
        RelativePath = "api/now/table/x_ppm_project_wbs_interface",
        Query = [
          sysparm_display_value = "true",
          sysparm_exclude_reference_link = "false",
          sysparm_query = "posidLIKE" & CompanyCodes & "^ORpbukrLIKE" & CompanyCodes & "^project_idISNOTEMPTY",
          sysparm_limit = Number.ToText(RecordLimit)
        ],
        Headers = [Accept = "application/json"]
      ]
    )
  ),
  InterfaceTable = Table.FromRecords({RawSource}),
  ExpandedList = Table.ExpandListColumn(InterfaceTable, "result"),
  ExpandedResult = Table.ExpandRecordColumn(ExpandedList, "result", {"project_id"}, {"project_id"}),
  FilteredRows = Table.SelectRows(ExpandedResult, each [project_id] <> ""),
  ExpandedProjectId = Table.ExpandRecordColumn(FilteredRows, "project_id", {"display_value", "link"}, {"project_display", "project_link"}),
  CleanedProjectIds = Table.TransformColumns(ExpandedProjectId, {{"project_link", each Text.AfterDelimiter(_, "/", 6), type text}}),
  UniqueProjectIds = List.Distinct(CleanedProjectIds[project_link]),

  // Step 2: Fetch full project info from pm_project using project sys_ids in batches with sys_updated_on filter
  fetchProjectBatch = (batch as list) =>
    let
        sysIdString = Text.Combine(batch, ","),
        queryFilter = 
            "sys_updated_onBETWEENjavascript:gs.dateGenerate('" & UpdatedOn & "','00:00:00')@" & 
            "javascript:gs.dateGenerate('" & UpdatedOn & "','23:59:59')^sys_idIN" & sysIdString,
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
        ExpandedRecords = Table.ExpandRecordColumn(ExpandedList, "result", {
          "short_description", "number", "u_accounting_master", "u_company_code", "sys_id", "u_project_state",
          "u_project_category", "u_bill_code", "u_ppm_debtor_relationship", "u_profit_center", "primary_portfolio",
          "percent_complete", "project_manager", "u_project_manager_deputee", "status", "u_complexity", "demand",
          "u_project_controller", "u_project_sponsor", "start_date", "end_date", "u_az_group_project_id", "u_ppm_request_envelope_id","sys_updated_on"
        })
    in
        ExpandedRecords,

  batches = List.Split(UniqueProjectIds, 50),
  allProjects = Table.Combine(List.Transform(batches, each fetchProjectBatch(_))),

  // Expand and transform columns
  Expand1 = Table.ExpandRecordColumn(allProjects, "u_accounting_master", {"display_value", "link"}, {"display_value", "link"}),
  Rename1 = Table.RenameColumns(Expand1, {{"display_value", "accounting_master"}, {"number", "project_number"}}),
  Remove1 = Table.RemoveColumns(Rename1, {"link"}),
  InsertCode = Table.AddColumn(Remove1, "Text Range", each Text.Middle([accounting_master], 0, 4), type text),
  Rename2 = Table.RenameColumns(InsertCode, {{"Text Range", "u_company_code_am"}}),
  AddFinalCompanyCode = Table.AddColumn(Rename2, "u_company_code_final", each if [u_company_code] = "" then [u_company_code_am] else [u_company_code]),
  RemoveTempCodes = Table.RemoveColumns(AddFinalCompanyCode, {"u_company_code", "u_company_code_am"}),
  RenameFinalCode = Table.RenameColumns(RemoveTempCodes, {{"u_company_code_final", "u_company_code"}}),
  AddExportFor = Table.AddColumn(RenameFinalCode, "ExportFor", each CompanyCode),

  ExpandPortfolio = Table.ExpandRecordColumn(AddExportFor, "primary_portfolio", {"display_value"}, {"primary_portfolio"}),
  ExpandController = Table.ExpandRecordColumn(ExpandPortfolio, "u_project_controller", {"display_value"}, {"u_project_controller"}),
  ExpandDeputee = Table.ExpandRecordColumn(ExpandController, "u_project_manager_deputee", {"display_value"}, {"u_project_manager_deputee"}),
  ExpandDemand = Table.ExpandRecordColumn(ExpandDeputee, "demand", {"display_value"}, {"demand"}),
  ExpandPM = Table.ExpandRecordColumn(ExpandDemand, "project_manager", {"display_value"}, {"project_manager"}),

  // Final transformation
  TransformTypes = Table.TransformColumnTypes(ExpandPM, {
    {"short_description", type text}, {"project_number", type text}, {"accounting_master", type text},
    {"sys_id", type text}, {"u_project_state", type text}, {"u_project_category", type text},
    {"u_bill_code", type text}, {"u_ppm_debtor_relationship", type text}, {"u_profit_center", type text},
    {"primary_portfolio", type text}, {"percent_complete", type text}, {"project_manager", type text},
    {"u_project_manager_deputee", type text}, {"status", type text}, {"u_complexity", type text},
    {"demand", type text}, {"u_project_controller", type text}, {"u_project_sponsor", type text},
    {"start_date", type text}, {"end_date", type text}, {"u_az_group_project_id", type text},
    {"u_ppm_request_envelope_id", type text}, {"u_company_code", type text}, {"ExportFor", type text}
  }),
  CleanErrors = Table.ReplaceErrorValues(TransformTypes, {
    {"short_description", null}, {"project_number", null}, {"accounting_master", null}, {"sys_id", null},
    {"u_project_state", null}, {"u_project_category", null}, {"u_bill_code", null},
    {"u_ppm_debtor_relationship", null}, {"u_profit_center", null}, {"primary_portfolio", null},
    {"percent_complete", null}, {"project_manager", null}, {"u_project_manager_deputee", null},
    {"status", null}, {"u_complexity", null}, {"demand", null}, {"u_project_controller", null},
    {"u_project_sponsor", null}, {"start_date", null}, {"end_date", null}, {"u_az_group_project_id", null},
    {"u_ppm_request_envelope_id", null}, {"u_company_code", null}, {"ExportFor", null}
  }),
  #"Filtered rows" = Table.SelectRows(CleanErrors, each ([sys_id] <> null))
in
  #"Filtered rows";
shared time_card_interface_set_a = let
  // Parameters
  UpdatedOn = UpdatedOn,
  RecordLimit = Number.FromText(NumberOfRecords),
  CompanyCodes = CompanyCode,

  // Filter for sys_updated_on on a specific day
  queryFilter =
      "sys_updated_onBETWEENjavascript:gs.dateGenerate('" & UpdatedOn & "','00:00:00')@" &
      "javascript:gs.dateGenerate('" & UpdatedOn & "','23:59:59')" &
      "^tc.state!=Locked" &
      "^tc.total!=0" &
      "^tc.categorySTARTSWITHProject" &
      "^tc.user.user_nameISNOTEMPTY" &
      "^rep_hours!=0" &
      "^tc.top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeIN" & CompanyCodes,

  // Fetch data
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
  
  // Parse results
  ConvertedToTable = Table.FromRecords({Source}),
  ExpandedResult1 = Table.ExpandListColumn(ConvertedToTable, "result"),
  ExpandedResult3 = Table.ExpandRecordColumn(ExpandedResult1, "result", {"tc", "comments", "postingdate", "state","sys_updated_on"}, {"tc", "comments", "postingdate", "state","sys_updated_on"}),

  // Extract tc fields
  ExpandedTc = Table.ExpandRecordColumn(ExpandedResult3, "tc", {"display_value", "link"}, {"tc.display_value", "tc.link"}),
  ExtractedSysId = Table.TransformColumns(ExpandedTc, {{"tc.link", each Text.AfterDelimiter(_, "/", 6), type text}}),

  // Rename and finalize
  Renamed = Table.RenameColumns(ExtractedSysId, {
    {"tc.link", "tc_sys_id"}, 
    {"tc.display_value", "week_starts_on"},  // Optional: could rename to something else if misleading
    {"postingdate", "posting_date"}
  }),
  AddedExportFor = Table.AddColumn(Renamed, "ExportFor", each CompanyCode),

  // Transform types and handle errors
  Typed = Table.TransformColumnTypes(AddedExportFor, {
    {"week_starts_on", type text}, {"comments", type text}, {"posting_date", type text}, {"state", type text}, {"ExportFor", type text}
  }),
  Cleaned = Table.ReplaceErrorValues(Typed, {
    {"week_starts_on", null}, {"comments", null}, {"posting_date", null}, {"state", null}, {"ExportFor", null}
  })
in
  Cleaned;
shared time_card_interface_set_b = let
  // Parameters
  UpdatedOn = UpdatedOn,
  RecordLimit = Number.FromText(NumberOfRecords),
  CompanyCodes = CompanyCode,
  Debtor = OrganizationalEntity,

  // Filter query using sys_updated_on
  queryFilter =
      "sys_updated_onBETWEENjavascript:gs.dateGenerate('" & UpdatedOn & "','00:00:00')@" &
      "javascript:gs.dateGenerate('" & UpdatedOn & "','23:59:59')" &
      "^tc.state!=Locked" &
      "^tc.total!=0" &
      "^tc.categorySTARTSWITHProject" &
      "^tc.user.user_nameISNOTEMPTY" &
      "^tc.user.u_debtorLIKE" & Debtor &
      "^rep_hours!=0" &
      "^tc.top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeNOT IN" & CompanyCodes,

  // Fetch the data
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

  // Parse results
  ConvertedToTable = Table.FromRecords({Source}),
  ExpandedResult1 = Table.ExpandListColumn(ConvertedToTable, "result"),
  ExpandedResult3 = Table.ExpandRecordColumn(ExpandedResult1, "result", {"tc", "comments", "postingdate", "state"}, {"tc", "comments", "postingdate", "state"}),

  // Expand tc fields
  ExpandedTc = Table.ExpandRecordColumn(ExpandedResult3, "tc", {"display_value", "link"}, {"tc.display_value", "tc.link"}),
  ExtractedSysId = Table.TransformColumns(ExpandedTc, {{"tc.link", each Text.AfterDelimiter(_, "/", 6), type text}}),

  // Rename and finalize
  Renamed = Table.RenameColumns(ExtractedSysId, {
    {"tc.link", "tc_sys_id"}, 
    {"tc.display_value", "week_starts_on"},  // Optional rename, or rename to sys_updated_on if more correct
    {"postingdate", "posting_date"}
  }),
  AddedExportFor = Table.AddColumn(Renamed, "ExportFor", each CompanyCodes),

  // Transform and clean
  Typed = Table.TransformColumnTypes(AddedExportFor, {
    {"week_starts_on", type text}, {"comments", type text}, {"posting_date", type text}, {"state", type text}, {"ExportFor", type text}
  }),
  Cleaned = Table.ReplaceErrorValues(Typed, {
    {"week_starts_on", null}, {"comments", null}, {"posting_date", null}, {"state", null}, {"ExportFor", null}
  })
in
  Cleaned;
shared time_card_interface_externals = let
  // Define the start and end dates
  UpdatedOn = UpdatedOn,
  RecordLimit = Number.FromText(NumberOfRecords),
  CompanyCodes = CompanyCode,
  Debtor = OrganizationalEntity,
  queryFilter = "timecard.user.u_debtorLIKE" & Debtor & "^" & "hours!=0^documentdateLIKE" &
    "sys_updated_onBETWEENjavascript:gs.dateGenerate('" & UpdatedOn & "','00:00:00')@" &
      "javascript:gs.dateGenerate('" & UpdatedOn & "','23:59:59')",
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
  #"Expanded result3" = Table.ExpandRecordColumn(#"Expanded result1", "result", {"timecard", "comments", "posting_date", "state", "documentdate", "raterole", "rate_per_hour"}, {"timecard", "comments", "posting_date", "state", "documentdate", "raterole", "rate_per_hour"}),
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
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "time_cards_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared time_cards = let
    // Parameters
    UpdateDate = UpdatedOn,  // Ensure string
    RecordLimit = Number.FromText(NumberOfRecords),
    CompanyCodes = CompanyCode,
    Debtor = OrganizationalEntity,

    // Base time_card fetching function using sys_updated_on
    fetchTimeCardsByDate = (filterText as text) =>
        let
            filter = "sys_updated_onBETWEENjavascript:gs.dateGenerate('" & UpdateDate & "','00:00:00')@javascript:gs.dateGenerate('" & UpdateDate & "','23:59:59')^" & filterText,
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
            records = Table.ExpandRecordColumn(expanded, "result", {
                "u_project_name","top_task","total","week_starts_on","monday","tuesday","wednesday","thursday",
                "friday","saturday","sunday","category","state","u_dept_cost_center","u_prj_unit_resp","u_rep_project",
                "u_timecard_source","u_rate_role","u_project_manager","u_resource_manager","resource_plan",
                "user","sys_id","sys_updated_by","sys_updated_on"
            }, {
                "u_project_name","top_task","total","week_starts_on","monday","tuesday","wednesday","thursday",
                "friday","saturday","sunday","category","state","u_dept_cost_center","u_prj_unit_resp","u_rep_project",
                "u_timecard_source","u_rate_role","u_project_manager","u_resource_manager","resource_plan",
                "user","tc_sys_id","sys_updated_by","sys_updated_on"
            }),
            userExpanded = Table.ExpandRecordColumn(records, "user", {"display_value", "link"}, {"user", "link"}),
            parsed = Table.TransformColumns(userExpanded, {{"link", each Text.AfterDelimiter(_, "/", 6), type text}}),
            renamed = Table.RenameColumns(parsed, {{"link", "user_sys_id"}})
        in
            renamed,

    // Load filtered time cards for all three sets
    timeCardSetA = fetchTimeCardsByDate("state!=Locked^total!=0^categorySTARTSWITHProject^top_task.ref_pm_project.u_accounting_master.u_master_approved!=Rejected^top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeIN" & CompanyCodes),
    timeCardSetB = fetchTimeCardsByDate("state!=Locked^total!=0^categorySTARTSWITHProject^top_task.ref_pm_project.u_accounting_master.u_master_approved!=Rejected^user.user_nameISNOTEMPTY^user.u_debtorLIKE" & Debtor & "^top_task.ref_pm_project.u_accounting_master.u_company_code.company_codeNOT IN" & CompanyCodes),
    timeCardSetC = fetchTimeCardsByDate("state!=Locked^total!=0^categoryNOT LIKEProject^user.user_nameISNOTEMPTY^user.u_debtorLIKE" & Debtor),

    allTimeCards = Table.Combine({timeCardSetA, timeCardSetB, timeCardSetC}),
    distinctUserSysIds = List.Distinct(allTimeCards[user_sys_id]),

    // Fetch sys_user data (batch removed)
    userQuery = "sys_idIN" & Text.Combine(distinctUserSysIds, ","),
    userResponse = Json.Document(Web.Contents("https://aztech.service-now.com", [
        RelativePath = "api/now/table/sys_user",
        Query = [
            sysparm_display_value = "true",
            sysparm_exclude_reference_link = "false",
            sysparm_query = userQuery,
            sysparm_limit = "5000"
        ],
        Headers = [Accept = "application/json"]
    ])),
    userTbl = Table.FromRecords({userResponse}),
    userExpanded = Table.ExpandListColumn(userTbl, "result"),
    allUsers = Table.ExpandRecordColumn(userExpanded, "result", {"user_name", "sys_id"}),

    // Fetch resource rate cards (batch removed)
    rateQuery = "user.sys_idIN" & Text.Combine(distinctUserSysIds, ","),
    rateResponse = Json.Document(Web.Contents("https://aztech.service-now.com", [
        RelativePath = "api/now/table/x_ppm_resource_rate_card",
        Query = [
            sysparm_display_value = "true",
            sysparm_exclude_reference_link = "false",
            sysparm_fields = "user,co_rate_role,active,sys_created_on",
            sysparm_query = rateQuery,
            sysparm_limit = "5000"
        ],
        Headers = [Accept = "application/json"]
    ])),
    rateTbl = Table.FromRecords({rateResponse}),
    rateExpandedList = Table.ExpandListColumn(rateTbl, "result"),
    rateExpanded = Table.ExpandRecordColumn(rateExpandedList, "result", {"user", "co_rate_role", "active", "sys_created_on"}, {"user", "co_rate_role", "active", "sys_created_on"}),
    roleExpanded = Table.ExpandRecordColumn(rateExpanded, "co_rate_role", {"display_value"}, {"rate"}),
    userExpanded2 = Table.ExpandRecordColumn(roleExpanded, "user", {"link"}, {"user_link"}),
    extracted = Table.TransformColumns(userExpanded2, {{"user_link", each Text.AfterDelimiter(_, "/", 6), type text}}),
    renamed = Table.RenameColumns(extracted, {{"user_link", "sys_id"}}),
    removedActive = Table.RemoveColumns(renamed, {"active"}),
    sorted = Table.Sort(removedActive, {{"sys_id", Order.Ascending}, {"sys_created_on", Order.Descending}}),
    grouped = Table.Group(sorted, {"sys_id"}, {{"TopRecord", each Table.FirstN(_, 1)}}),
    finalRates = Table.ExpandTableColumn(grouped, "TopRecord", {"rate", "sys_created_on"}),

    // Combine all
    allRateCardsClean = Table.RenameColumns(Table.Distinct(finalRates, {"sys_id"}), {{"sys_id", "rate_sys_id"}}),
    joinWithUsers = Table.Join(allTimeCards, "user_sys_id", allUsers, "sys_id", JoinKind.LeftOuter),
    joinWithUsersClean = Table.RemoveColumns(joinWithUsers, {"sys_id"}),
    joinWithRates = Table.Join(joinWithUsersClean, "user_sys_id", allRateCardsClean, "rate_sys_id", JoinKind.LeftOuter),
    joinWithRatesClean = Table.RemoveColumns(joinWithRates, {"sys_created_on", "rate_sys_id"}),
    withExportInfo = Table.AddColumn(joinWithRatesClean, "ExportFor", each CompanyCode),
    finalOutput = Table.AddColumn(withExportInfo, "RefreshDate", each DateTime.LocalNow())
in
    finalOutput;
shared NumberOfRecords = "100" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared OrganizationalEntity = "2000014578 Allianz Technology s.r.o." meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type any];
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "time_card_interface_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared time_card_interface = let
  Source = Table.Combine({time_card_interface_externals, time_card_interface_set_a, time_card_interface_set_b})
in
  Source;
shared time_card_interface_table = "2024_cz45_time_card_interface" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared time_card_interface_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data],
  TableNavigation = Navigation_2{[Id = time_card_interface_table, ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared u_ppm_accounting_element_table = "2024_cz45_u_ppm_accounting_element" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared u_ppm_accounting_element_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "bd777a6f-f8b4-4d15-bcad-2edf46657ec3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "12187422-6c42-47a0-a6eb-591b9f7dc6b5"]}[Data],
  TableNavigation = Navigation_2{[Id = u_ppm_accounting_element_table, ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared pm_projects_table = "2024_cz45_pm_projects" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared extrernal_rate_cards_table = "2024_cz45_extrernal_rate_cards" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
shared time_cards_table = "2024_cz45_time_cards" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
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
  TableNavigation = Navigation_2{[Id = time_cards_table, ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
shared UpdatedOn = "2025-07-10" meta [IsParameterQuery = true, IsParameterQueryRequired = false, Type = type text];
